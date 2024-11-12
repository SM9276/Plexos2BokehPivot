import os
import traceback
import clr
import sys
import csv
import pandas as pd
import concurrent.futures
import zipfile
from xml.etree import ElementTree as ET
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import io

# Load PLEXOS assemblies
plexos_path = 'C:/Program Files/Energy Exemplar/PLEXOS 10.0 API'
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

def get_property_enum_map(collection_name, solution_file_path):
    """
    Get a dictionary of property names and their enums for a specific collection.

    Args:
    - collection_name: Name of the collection to retrieve properties for (e.g., "Generators").
    - solution_file_path: Path to the solution file to connect to.

    Returns:
    - property_enum_map: Dictionary of properties with successful enum retrievals.
    """
    sol = Solution()
    property_enum_map = {}
    
    # Derive classid based on the collection name
    if collection_name == "EmissionGenerators":
        classid = "Emission"
    else:
        classid = collection_name[:-1] if collection_name.endswith('s') else collection_name

    try:
        # Connect to the specified solution file
        sol.Connection(solution_file_path)
        
        # Retrieve available properties and their enums
        properties = sol.GetReportedProperties()
        for prop in properties:
            try:
                # Obtain the enum ID
                enum_id = sol.PropertyName2EnumId("System", classid, collection_name, prop)
                enum_id = str(enum_id) # P10 API takes the string of the integer of the enum as input for queryToList()
                property_enum_map[prop] = enum_id
            except Exception as e:
                print(f"Could not retrieve enum for property '{prop}': {e}")
    except Exception as e:
        print(f"Error retrieving properties for collection {collection_name}: {e}")
    finally:
        sol.Close()
    
    return property_enum_map

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
        xml_file = None  

        for name in list_of_files_in_zip:
            if name.endswith("Solution.xml"):
                xml_file = name
                break 
        if not xml_file:
            print(f"No XML file found in {sol_file}")
            return None, None

        try:
            with zf.open(xml_file) as xml_fp:
                tree = ET.parse(xml_fp)
                root = tree.getroot()
                namespace = {'ns': 'http://tempuri.org/SolutionDataset.xsd'}
                date_from = datetime.max
                date_to = datetime.min
                line_count = 0
                for t_period in root.findall('.//ns:t_period_0', namespace):
                    datetime_str = t_period.find('ns:datetime', namespace).text

                    if print_enabled:
                        print("Datetime string:", datetime_str) 

                    try:
 
                        datetime_obj = datetime.strptime(datetime_str, '%d/%m/%Y %H:%M:%S')
                    except Exception as e:
                        print("Error parsing datetime string:", e)
                        continue

                    if datetime_obj < date_from:
                        date_from = datetime_obj
                    if datetime_obj > date_to:
                        date_to = datetime_obj
                    line_count += 1
        except Exception as e:
            print(f"Error while processing the XML: {e}")
            input('Press any key to continue...')

    return date_from, date_to

def append_files(output_folder):
    """
    Function to append '_append' files to their corresponding CSV files.
    
    Args:
    - output_folder: Path to the output folder.
    """
    # Traverse the output folder
    for root, dirs, files in os.walk(output_folder):
        for file in files:
            # Check for '_append' in the file name
            if '_append' in file:
                append_file_path = os.path.join(root, file)
                original_file_path = append_file_path.replace('_append', '')

                # Check if the original file exists
                if os.path.exists(original_file_path):
                    try:
                        # Read the content of the _append file and the original file
                        append_df = pd.read_csv(append_file_path)
                        original_df = pd.read_csv(original_file_path)

                        # Append the data
                        combined_df = pd.concat([original_df, append_df])

                        # Save back to the original file
                        combined_df.to_csv(original_file_path, index=False)
                        print(f"Appended {file} to {os.path.basename(original_file_path)}")

                    except Exception as e:
                        print(f"Error appending {file} to {os.path.basename(original_file_path)}: {e}")
                else:
                    print(f"Original file not found for {file}, skipping append.")

# def process_collection_chunk(collection, input_folder, output_folder, sol_files, property, period_name):
def process_collection_chunk(collection, input_folder, output_folder, sol_files, property_enum, period_name, property_name):

    """
    Function to process a collection of data.

    Args:
    - collection: The collection name.
    - input_folder: Path to the input folder.
    - output_folder: Path to the output folder.
    - sol_files: List of solution files.
    """
    import System


    # QueryToCSV Inputs
    append         = True
    simulation     = SimulationPhaseEnum.LTPlan
    periodEnum     = getattr(PeriodEnum, f'{period_name}')  
    collectionEnum = int(getattr(CollectionEnum, f'System{collection}') if collection != 'EmissionGenerators' else CollectionEnum.EmissionGenerators)
    parentName     = "" 
    childName      = ""
    seriesEnum     = SeriesTypeEnum.Properties
    property       = property_enum
    timeSliceList  = ""
    sampleList     = ""
    modelName      = ""
    aggregation    = AggregationTypeEnum.CategoryAggregation
    category       = ""
    seperator      = ","
    operation      = OperationTypeEnum.SUM

    # Define heuristic for output filenames based on collection and property names
    def get_output_filename(collection, property_name):
        if collection == "Batteries":
            if property_name == "Generation":
                return "gen_ann_append.csv"
            elif property_name == "Load":
                return "bat_load.csv"
            elif property_name == "Capacity Built":
                return "cap_append.csv"
        elif collection == "Generators":
            if property_name == "Generation":
                return "gen_ann.csv"
            elif property_name == "Capacity Built":
                return "cap.csv"
        elif collection == "Emissions" and property_name == "Emission Rate":
            return "emit_r.csv"
        return f"{property_name.lower().replace(' ', '_')}.csv"  # Default naming convention
    
    output_filename = get_output_filename(collection, property_name)

    # Common columns for all files
    columns = ["category_name", "p1", "year", "month", "day", "hour", "value"]
    
    # Set the number of files per line
    files_per_row = 3

    # Display the collection and period information
    print(f"Processing collection '{collection}' with PeriodEnum {period_name} and sol files:")

    # Format the solution files output
    for i in range(0, len(sol_files), files_per_row):
        # Join up to files_per_row elements for each line
        print("    " + ", ".join(sol_files[i:i + files_per_row]))

    sol = Solution()

    # Iterate through sol_files
    for sol_file in sol_files:
        sol_file_path = os.path.join(input_folder, sol_file)
        print(sol_file_path)
        folder_name = "outputs"
        
        solution_name = os.path.splitext(sol_file)[0]
        solution_output_folder = os.path.join(output_folder, period_name, solution_name, folder_name)
        output_csv_file = os.path.join(solution_output_folder, output_filename)

        # Check if the solution file exists, delete it if it does
        if os.path.exists(output_csv_file):
            os.remove(output_csv_file)  # Deletes the specific solution file
        os.makedirs(solution_output_folder, exist_ok=True)

        sol.Connection(sol_file_path)
        print(f'Processing {collection} for {sol_file}...')

        try:
            if period_name == "Interval" and collection == "Generators":
                print('Interval query detected. Partitioning data...')
                date_from, date_to = find_horizon(sol_file_path)
                print(f"horizon dates: {date_from}, {date_to}")

                # Partition data by year
                current_date = date_from

                while current_date <= date_to:
                    end_of_year = (current_date + relativedelta(years=1)) - timedelta(hours=1)
                    if end_of_year > date_to:
                        end_of_year = date_to

                    TS0 = current_date.strftime('%m/%d/%Y %I:%M:%S %p').replace('/0', '/').lstrip("0").replace(" 0", " ")
                    TS1 = end_of_year.strftime('%m/%d/%Y %I:%M:%S %p').replace('/0', '/').lstrip("0").replace(" 0", " ")

                    start = getattr(getattr(System, "DateTime"), "Parse")(TS0)
                    end = getattr(getattr(System, "DateTime"), "Parse")(TS1)
                    print(f'Processing data from {TS0} to {TS1}...')

                    result = sol.QueryToList(
                        simulation,
                        collectionEnum,
                        parentName,
                        childName,
                        periodEnum,
                        seriesEnum,
                        property,
                        start,
                        end,
                        timeSliceList,
                        sampleList,
                        modelName,
                        aggregation,
                        category,
                        seperator,
                        operation
                    )

                    with open(output_csv_file, 'a', newline='') as csvfile:
                        csvwriter = csv.writer(csvfile)

                        # Write the header only if the file is being created for the first time
                        if not os.path.exists(output_csv_file) or os.path.getsize(output_csv_file) == 0:
                            csvwriter.writerow(columns)

                        for row in result:
                            try:
                                row_data = [getattr(row, col, '') for col in ["category_name", "value"]]
                                row_data.insert(1, "p1")  # Add "p1" in the second column

                                date_str = str(row._date)
                                date_parts = date_str.split(' ')
                                date_component = date_parts[0].split('/')
                                time_component = date_parts[1].split(':') if len(date_parts) > 1 else [0]

                                month = date_component[0]
                                day = date_component[1]
                                year = date_component[2]
                                hour = int(time_component[0])

                                if 'PM' in date_str and hour != 12:
                                    hour += 12
                                elif 'AM' in date_str and hour == 12:
                                    hour = 0

                                row_data.insert(2, year)
                                row_data.insert(3, month)
                                row_data.insert(4, day)
                                row_data.insert(5, hour)

                                csvwriter.writerow(row_data)

                            except Exception as e:
                                print(f"Error processing row: {e}")

                    print(f'Results saved to {output_csv_file}')
                    current_date += relativedelta(years=1)
            else:
                # Non-interval case for Generators
                print(f'Processing entire horizon for {collection} for {sol_file}...')
                date_from, date_to = find_horizon(sol_file_path)
                TS0 = date_from.strftime('%m/%d/%Y %I:%M:%S %p').replace('/0', '/').lstrip("0").replace(" 0", " ")
                TS1 = date_to.strftime('%m/%d/%Y %I:%M:%S %p').replace('/0', '/').lstrip("0").replace(" 0", " ")

                start = getattr(getattr(System, "DateTime"), "Parse")(TS0)
                end = getattr(getattr(System, "DateTime"), "Parse")(TS1)
                
                result = sol.QueryToList(
                    simulation,
                    collectionEnum,
                    parentName,
                    childName,
                    periodEnum,
                    seriesEnum,
                    property,
                    start,
                    end,
                    timeSliceList,
                    sampleList,
                    modelName,
                    aggregation,
                    category,
                    seperator,
                    operation
                )

                with open(output_csv_file, 'a', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile)

                    # Write the header only if the file is being created for the first time
                    if not os.path.exists(output_csv_file) or os.path.getsize(output_csv_file) == 0:
                        csvwriter.writerow(columns)

                    for row in result:
                        try:
                            row_data = [getattr(row, col, '') for col in ["category_name", "value"]]
                            row_data.insert(1, "p1")  # Add "p1" in the second column

                            date_str = str(row._date)
                            date_parts = date_str.split(' ')
                            date_component = date_parts[0].split('/')
                            time_component = date_parts[1].split(':') if len(date_parts) > 1 else [0]

                            month = date_component[0]
                            day = date_component[1]
                            year = date_component[2]
                            hour = int(time_component[0])

                            if 'PM' in date_str and hour != 12:
                                hour += 12
                            elif 'AM' in date_str and hour == 12:
                                hour = 0

                            row_data.insert(2, year)
                            row_data.insert(3, month)
                            row_data.insert(4, day)
                            row_data.insert(5, hour)

                            csvwriter.writerow(row_data)

                        except Exception as e:
                            print(f"Error processing row: {e}")

                print(f'Results saved to {output_csv_file}')

        except Exception as e:
            error_message = f"Error processing {collection} for {sol_file_path}: {e}"
            print(error_message)
            error_file = "error_log.txt"
            with open(error_file, "a") as f:
                f.write(f"Error processing {collection} for {sol_file_path}: {e}\n")
                f.write(traceback.format_exc() + "\n")
            input('Press any key to continue...')
        finally:
            sol.Close()

# Get collections from the config file
collections_str = "Generators Emissions Batteries"
collections = collections_str.split() if collections_str else []

# Declare the collections to process
print('Collections to process: ')
for collection in collections:
    print(collection)

properties_to_query = {
    # "Generators": ["Generation", "Installed Capacity"],
    "EmissionGenerators": ["Production"],
    # "Batteries": ["Generation", "Load", "InStalled Capacity"]
}

# Get input and output folder paths
input_folder = "PlexosSolutions"
output_folder = "runs"

sol_files = [f for f in os.listdir(input_folder) if f.endswith('.zip')]

for sol_file in sol_files:
    solution_file_path = os.path.join(input_folder, sol_file)

# Check if there are no solution files
if not sol_files:
    print(f'No solution files found in {input_folder}. Exiting...')
    input('Press any key to continue...')
else:
    period_name = input("Please enter 'FiscalYear' or 'Interval': ")
    
    for collection in collections:

        property_enum_map = get_property_enum_map(collection, solution_file_path)

        # Process only the specified properties for this collection
        if collection in properties_to_query:
            for prop_name in properties_to_query[collection]:
                if prop_name in property_enum_map:
                    property_enum = property_enum_map[prop_name]
                    process_collection_chunk(collection, input_folder, output_folder, sol_files, property_enum, period_name, prop_name)
                else:
                    print(f"Property '{prop_name}' not found in the {collection} collection.")
    
        # for prop_name in properties_to_query:
        #     if prop_name in property_enum_map[collection]:
        #         property_enum = property_enum_map[collection][prop_name]
        #         process_collection_chunk(collection, input_folder, output_folder, sol_files, property_enum, period_name, prop_name)
        #     else:
        #         print(f"Property '{prop_name}' not found in the {collection} collection.")        # Retrieve property-enum mapping for the current solution file and collection

    print("Appending '_append' files to corresponding CSVs...")
    append_files(output_folder)
