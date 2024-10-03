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



def process_collection_chunk(collection, input_folder, output_folder, sol_files, propertyList, date_from=None, date_to=None):
    """
    Function to process a collection of data.

    Args:
    - collection: The collection name.
    - input_folder: Path to the input folder.
    - output_folder: Path to the output folder.
    - sol_files: List of solution files.
    """
    import System
    
    period_enum_value = "Interval"
    
    #QueryToCSV Inputs
    append         = True
    simulation     = SimulationPhaseEnum.LTPlan
    periodEnum     = getattr(PeriodEnum, f'{period_enum_value}')  
    collectionEnum = int(getattr(CollectionEnum, f'System{collection}') if collection != 'EmissionGenerators' else CollectionEnum.EmissionGenerators)
    parentName     = "" 
    childName      = ""
    seriesEnum     = SeriesTypeEnum.Properties
    timeSliceList  = ""
    sampleList     = ""
    modelName      = ""
    aggregation    = AggregationTypeEnum.CategoryAggregation
    category       = ""
    seperator      = ","
    operation      = OperationTypeEnum.SUM
    if collection == "Batteries":
        if property == "5":
            output_filename = "gen_ann_append.csv"
        elif property == "6":
            output_filename = "bat_load.csv"
        elif property == "82":
            output_filename = "cap_append.csv"
        else:
            return  # Skip unknown property
    elif collection == "Generators":
        if property == "2":
            output_filename = "gen_ann.csv"
        elif property == "240":
            output_filename = "cap.csv"
        else:
            return  # Skip unknown property
    elif collection == "Emissions" and property == "1":
        output_filename = "emit_r.csv"
    else:
        return  # Skip unknown collection or property

    # Common columns for all files
    columns = ["category_name", "p1", "year", "month", "day", "hour", "value"]
    # Import the System namespace inside the function
    print(f"Processing collection '{collection}' with PeriodEnum {period_enum_value} and sol files: {sol_files}")

    sol = Solution()

    # Iterate through sol_files
    for sol_file in sol_files:
        sol_file_path = os.path.join(input_folder, sol_file)
        print(sol_file_path)
        folder_name = "Outputs"
        
        solution_name = os.path.splitext(sol_file)[0]
        solution_output_folder = os.path.join(output_folder, period_enum_value , solution_name, folder_name)
	    # Define the solution file path
        solution_file_path = os.path.join(solution_output_folder, output_filename)

	    # Check if the solution file exists, delete it if it does
        if os.path.exists(solution_file_path):
            os.remove(solution_file_path)  # Deletes the specific solution file

        os.makedirs(solution_output_folder, exist_ok=True)

        sol.Connection(sol_file_path)
        print(f'Processing {collection} for {sol_file}...')
        try:
            if period_enum_value == "Interval" and collection == "Generators":
                print('Interval query detected. Partitioning data...')

                date_from, date_to = find_horizon(sol_file_path)
                print(f"horizon dates: {date_from}, {date_to}")

                # Partition data by month
                current_date = date_from

                while current_date <= date_to:
                    end_of_year = (current_date + relativedelta(years=1)) - timedelta(hours=1)

                    if end_of_year > date_to:
                        end_of_year = date_to

                    TS0 = current_date.strftime('%m/%d/%Y %I:%M:%S %p').replace('/0', '/').lstrip("0").replace(" 0", " ")
                    TS1 = end_of_year.strftime('%m/%d/%Y %I:%M:%S %p').replace('/0', '/').lstrip("0").replace(" 0", " ")

                    start = getattr(getattr(System, "DateTime"), "Parse")(TS0)  # Object DateFrom[ = None],
                    end = getattr(getattr(System, "DateTime"), "Parse")(TS1) # Object DateTo[ = None],
                    print(f'Processing data from {TS0} to {TS1}...')

                    # Dynamically generate output CSV file path based on partition time range
                    result = sol.QueryToList(
                    simulation,       # SimulationPhaseEnum
                    collectionEnum,   # Int32
                    parentName,       # String
                    childName,        # String
                    periodEnum,       # PeriodEnum
                    seriesEnum,       # SeriesTypeEnum
                    property,     # String
                    start,            # DateTime object for the start of the year
                    end,              # DateTime object for the end of the year
                    timeSliceList,    # String
                    sampleList,       # String
                    modelName,        # String
                    aggregation,      # AggregationTypeEnum
                    category,         # String
                    seperator,        # String
                    operation        # OperationTypeEnum    simulation,       # SimulationPhaseEnum
                    )
                    output_csv_file = os.path.join(solution_output_folder, output_filename)

                    with open(output_csv_file, 'a', newline='') as csvfile:
                        csvwriter = csv.writer(csvfile)
                        csvwriter.writerow(columns)

                        for row in result:
                            try:
                                row_data = [getattr(row, col, '') for col in ["category_name", "value"]]
                                row_data.insert(1, "p1")  # Add "p1" in the second column

                        # Split the _date and extract year, month, day, and hour
                                date_str = str(row._date)

                        # Corrected splitting of date
                                date_parts = date_str.split(' ')
                                date_component = date_parts[0].split('/')  # Assuming date format is MM/DD/YYYY
                                time_component = date_parts[1].split(':') if len(date_parts) > 1 else [0]  # Time, if exists

                                month = date_component[0]  # Month
                                day = date_component[1]    # Day
                                year = date_component[2]   # Year

                                hour = int(time_component[0])  # Hour part

                        # Adjust hour based on AM/PM logic if needed
                                if 'PM' in date_str and hour != 12:
                                    hour += 12
                                elif 'AM' in date_str and hour == 12:
                                    hour = 0

                        # Corrected row data
                                row_data.insert(2, year)   # Insert year
                                row_data.insert(3, month)  # Insert month
                                row_data.insert(4, day)    # Insert day
                                row_data.insert(5, hour)   # Insert hour

                        # The "value" is already in the last column (index 6 after p1 insert)
                                csvwriter.writerow(row_data)

                            except Exception as e:
                                print(f"Error processing row: {e}")
            
                    print(f'Results saved to {output_csv_file}')


                    current_date += relativedelta(years=1)


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
collections_str = "Generators"
collections = collections_str.split() if collections_str else []

# Declare the collections to process
print('Collections to process: ')
for collection in collections:
    print(collection)

# Get input and output folder paths
input_folder = "PlexosSolutions"
output_folder = "runs"



sol_files = [f for f in os.listdir(input_folder) if f.endswith('.zip')]


# Check if there are no solution files
if not sol_files:
    print(f'No solution files found in {input_folder}. Exiting...')
    input('Press any key to continue...')
else:
    try:
        # Run sequentially
        print("Running sequentially...")
        propertyList = "2,240"
        for property in propertyList.split(','):
                print(property)
                process_collection_chunk(collection, input_folder, output_folder, sol_files, property)
    except Exception as e:
        print(f"Parallel execution failed with error: {e}")
