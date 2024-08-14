import os
import traceback
import clr
import sys
import pandas as pd
import concurrent.futures
import zipfile
from xml.etree import ElementTree as ET
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import io

# Load PLEXOS assemblies
plexos_path = 'C:/Program Files/Energy Exemplar/PLEXOS 9.0 API'
sys.path.append(plexos_path)
clr.AddReference('PLEXOS_NET.Core')
clr.AddReference('EEUTILITY')
clr.AddReference('EnergyExemplar.PLEXOS.Utility')
clr.AddReference('PLEXOSCommon')

# Import from .NET assemblies (both PLEXOS and system)
from PLEXOS_NET.Core import *
from EEUTILITY.Enums import *
from EnergyExemplar.PLEXOS.Utility.Enums import *
from PLEXOSCommon.Enums import *
from System import DateTime
from System import *

def find_horizon(sol_file, print_enabled=False):
    """
    Function to find a model's horizon of dates in the XML file within a specified zip file.

    Args:
    - sol_file: Path to the solution zip file.
    - print_enabled: Flag to enable printing.

    Returns:
    - date_from: Start date found in the XML file.
    - date_to: End date found in the XML file.
    """
    with zipfile.ZipFile(sol_file) as zf:
        list_of_files_in_zip = zf.namelist()
        xml_file = None  # Initialize xml_file variable outside the loop

        for name in list_of_files_in_zip:
            if name.endswith(f"Solution.xml"):
                xml_file = name
                break  # Break out of loop once file is found
        
        if not xml_file:
            print(f"No XML file found in {sol_file}")
            return None, None

        try:
            with zf.open(xml_file) as xml_fp:
                tree = ET.parse(xml_fp)
                root = tree.getroot()

                # Define the namespace
                namespace = {'ns': 'http://tempuri.org/SolutionDataset.xsd'}

                # Initialize date_from and date_to with the maximum and minimum possible dates
                date_from = datetime.max
                date_to = datetime.min

                # Counter for controlling output
                line_count = 0

                # Traverse through the XML tree to find relevant datetime information
                for t_period in root.findall('.//ns:t_period_0', namespace):
                    datetime_str = t_period.find('ns:datetime', namespace).text

                    if print_enabled:
                        print("Datetime string:", datetime_str)  # Print datetime string

                    try:
                        # Adjust the format string to handle the inconsistency
                        datetime_obj = datetime.strptime(datetime_str, '%d/%m/%Y %H:%M:%S')
                    except Exception as e:
                        print("Error parsing datetime string:", e)
                        continue

                    # Update date_from and date_to based on the current datetime
                    if datetime_obj < date_from:
                        date_from = datetime_obj
                    if datetime_obj > date_to:
                        date_to = datetime_obj

                    # Increment line count
                    line_count += 1
        except Exception as e:
            print(f"Error while processing the XML: {e}")
            input('Press any key to continue...')

    return date_from, date_to

# Read config from CSV file
basedir = os.getcwd()
config = pd.read_csv(os.path.join(basedir, 'config.csv')).fillna(0)

def get_config_value(assumption, default=None):
    """
    Function to get a configuration value from a CSV file.

    Args:
    - assumption: The assumption key.
    - default: Default value if the assumption key is not found.

    Returns:
    - The value corresponding to the assumption key, or the default value if not found.
    """
    value = config[config['assumption'] == assumption]['value'].reset_index(drop=True).values[0]
    if value == 0:
        value = config[config['assumption'] == assumption]['default value'].reset_index(drop=True).values[0]
    return value if value != 0 else default

def process_collection(collection, input_folder, output_folder, sol_files, date_from=None, date_to=None):
    """
    Function to process a collection of data.

    Args:
    - collection: The collection name.
    - input_folder: Path to the input folder.
    - output_folder: Path to the output folder.
    - sol_files: List of solution files.
    """
    # Import the System namespace inside the function
    import System
    period_enum_key = f"{collection}_periodEnum"
    period_enum_value = get_config_value(period_enum_key)
    print(f"Processing collection '{collection}' with PeriodEnum {period_enum_value} and sol files: {sol_files}")

    sol = Solution()

    # Iterate through sol_files
    for sol_file in sol_files:
        sol_file_path = os.path.join(input_folder, sol_file)
        print(sol_file_path)
        
        # Use the output_folder directly instead of creating a /runs/ subfolder
        solution_name = os.path.splitext(sol_file)[0]
        solution_output_folder = os.path.join(output_folder, solution_name)
        os.makedirs(solution_output_folder, exist_ok=True)

        sol.Connection(sol_file_path)
        print(f'Processing {collection} for {sol_file}...')
        try:
            if period_enum_value == "Interval":
                print('Interval query detected. Partitioning data...')

                date_from, date_to = find_horizon(sol_file_path)
                print(f"horizon dates: {date_from}, {date_to}")
                
                # Partition data by month
                current_date = date_from

                while current_date <= date_to:
                    end_of_month = (current_date + relativedelta(months=1)) - timedelta(hours=1)

                    if end_of_month > date_to:
                        end_of_month = date_to

                    TS0 = current_date.strftime('%m/%d/%Y %I:%M:%S %p').replace('/0', '/').lstrip("0").replace(" 0", " ")
                    TS1 = end_of_month.strftime('%m/%d/%Y %I:%M:%S %p').replace('/0', '/').lstrip("0").replace(" 0", " ")

                    print(f'Processing data from {TS0} to {TS1}...')

                    # Dynamically generate output CSV file path based on partition time range
                    output_csv_file = os.path.join(solution_output_folder, f'{collection}_{current_date.strftime("%Y-%m")}.csv')

                    sol.QueryToCSV(
                        output_csv_file,  # String strCSVFile,
                        False,  # Boolean bAppendToFile,   Use 'True' to append to the file
                        SimulationPhaseEnum.LTPlan,
                        getattr(CollectionEnum, f'System{collection}') if collection != 'EmissionGenerators' else CollectionEnum.EmissionGenerators,  # CollectionEnum CollectionId, note: all collections are prefixed with 'System' except for EmissionGenerators
                        '',  # String ParentName,
                        '',  # String ChildName,
                        getattr(PeriodEnum, f'{period_enum_value}'),  # PeriodEnum PeriodTypeId,
                        SeriesTypeEnum.Properties,  # SeriesTypeEnum SeriesTypeId,
                        '',  # String PropertyList[ = None], 
                        getattr(getattr(System, "DateTime"), "Parse")(TS0),  # Object DateFrom[ = None],
                        getattr(getattr(System, "DateTime"), "Parse")(TS1),  # Object DateTo[ = None],
                    )

                    # Read the generated CSV file to add new columns
                    df_bat = pd.read_csv(output_csv_file)

                    if '_date' in df_bat.columns:
                        # Extract year, hour, and month from the _date column
                        df_bat['year'] = df_bat['_date'].str.split(' ').str[0].str.split('/').str[2]
                        df_bat['hour'] = df_bat['_date'].str.split(' ').str[1].str.split(':').str[0]
                        df_bat['month'] = df_bat['_date'].str.split('/').str[0]

                        # Save the updated DataFrame back to the same CSV file
                        df_bat.to_csv(output_csv_file, index=False)

                    # Convert absolute path to relative path
                    relative_sol_file_path = os.path.relpath(sol_file_path, input_folder)
                    relative_output_csv_file = os.path.relpath(output_csv_file, solution_output_folder)

                    print(f'Results from {relative_sol_file_path} saved to {relative_output_csv_file} for {current_date} to {end_of_month}')

                    current_date += relativedelta(months=1)
            else:
                # Run the query as before for non-Interval periods
                output_csv_file = os.path.join(solution_output_folder, f'{collection}.csv')
                sol.QueryToCSV(
                    output_csv_file,  # String strCSVFile,
                    False,  # Boolean bAppendToFile,   Use 'True' to append to the file
                    SimulationPhaseEnum.LTPlan,
                    getattr(CollectionEnum, f'System{collection}') if collection != 'EmissionGenerators' else CollectionEnum.EmissionGenerators,  # CollectionEnum CollectionId, note: all collections are prefixed with 'System' except for EmissionGenerators
                    '',  # String ParentName,
                    '',  # String ChildName,
                    getattr(PeriodEnum, f'{period_enum_value}'),  # PeriodEnum PeriodTypeId,
                    SeriesTypeEnum.Properties,  # SeriesTypeEnum SeriesTypeId,
                )

                # Read the generated CSV file to add new columns
                df_bat = pd.read_csv(output_csv_file)

                if '_date' in df_bat.columns:
                    # Extract year, hour, and month from the _date column
                    df_bat['year'] = df_bat['_date'].str.split(' ').str[0].str.split('/').str[2]
                    df_bat['hour'] = df_bat['_date'].str.split(' ').str[1].str.split(':').str[0]
                    df_bat['month'] = df_bat['_date'].str.split('/').str[0]

                    # Save the updated DataFrame back to the same CSV file
                    df_bat.to_csv(output_csv_file, index=False)

                # Convert absolute path to relative path
                relative_sol_file_path = os.path.relpath(sol_file_path, input_folder)
                relative_output_csv_file = os.path.relpath(output_csv_file, solution_output_folder)

                print(f'Results from {relative_sol_file_path} saved to {relative_output_csv_file}')

        except Exception as e:
            error_message = f"Error processing {collection} for {sol_file_path}: {e}"
            print(error_message)
            # Log the error message to a file
            error_file = "error_log.txt"
            with open(error_file, "a") as f:
                f.write(f"Error processing {collection} for {sol_file_path}: {e}\n")
                f.write(traceback.format_exc() + "\n")
            input('Press any key to continue...')
        finally:
            # Important to Close() the Solution to clear working storage.
            sol.Close()

# Get collections from the config file
collections_str = get_config_value('collections')
collections = collections_str.split() if collections_str else []

# Declare the collections to process
print(f'Collections to process: ')
for collection in collections:
    print(collection)

# Get input and output folder paths
input_folder = get_config_value('input_folder')
output_folder = get_config_value('output_folder')

# Get solution filenames or use all solutions
check_sol_files = config[config['assumption'] == 'solution_filename']['value'].tolist()

# Check if there are no solution files specified
if not check_sol_files or check_sol_files == [0]:
    print('No specific solution files specified. Using all solution files.')
    sol_files = [f for f in os.listdir(input_folder) if f.endswith('.zip')]
else:
    # Convert the list of filenames to strings
    sol_files = [str(filename) for filename in check_sol_files]
    
# Read the parallel execution decision from the config file
run_in_parallel = get_config_value('run_in_parallel').lower() == 'yes'

# Check if there are no solution files
if not sol_files:
    print(f'No solution files found in {input_folder}. Exiting...')
    input('Press any key to continue...')
else:
    try:
        if run_in_parallel:
            # Determine the number of available CPU cores
            available_cores = os.cpu_count()
            # Calculate 75% of the available cores and round as necessary
            parallel_cores = max(1, round(0.75 * available_cores))
            print(f"Running in parallel with {parallel_cores} cores...")
            # Use ThreadPoolExecutor for parallel execution with the calculated number of cores
            with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_cores) as executor:
                executor.map(process_collection, collections, [input_folder] * len(collections), [output_folder] * len(collections), [sol_files] * len(collections))
        else:
            # Run sequentially
            print("Running sequentially...")
            for collection in collections:
                process_collection(collection, input_folder, output_folder, sol_files)
    except Exception as e:
        print(f"Parallel execution failed with error: {e}")
        print("Switching to sequential execution...")
        # Run sequentially
        for collection in collections:
            process_collection(collection, input_folder, output_folder, sol_files)
