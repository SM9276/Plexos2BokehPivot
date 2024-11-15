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
import json
import re

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

def parse_collection_enum(collection_enum_str):
    """
    Parse the CollectionEnum string into a dictionary mapping collection_id to collection_name.
    """
    collection_mapping = {}
    lines = collection_enum_str.strip().split('\n')
    for line in lines:
        line = line.strip()
        if not line or line.startswith('CollectionEnum') or line.startswith('System.'):
            continue
        match = re.match(r'(\w+)\s*=\s*(\d+)', line)
        if match:
            name, cid = match.groups()
            collection_mapping[int(cid)] = name
    return collection_mapping

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

def process_collection_chunk(collection_id, collection_name, input_folder, output_folder, sol_files, property_id, period_enum_value):
    """
    Function to process a collection of data.

    Args:
    - collection_id: The collection ID.
    - collection_name: The collection name.
    - input_folder: Path to the input folder.
    - output_folder: Path to the output folder.
    - sol_files: List of solution files.
    - property_id: The property ID to process.
    - period_enum_value: 'FiscalYear' or 'Interval'
    """
    import System

    # QueryToCSV Inputs
    append         = True
    simulation     = SimulationPhaseEnum.LTPlan
    periodEnum     = getattr(PeriodEnum, f'{period_enum_value}')  
    collectionEnum = collection_id  # Use collection_id directly
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

    # Map collection and property IDs to output filenames (you may need to adjust this mapping)
    output_filename = f"collection_{collection_id}_property_{property_id}.csv"

    # Common columns for all files
    columns = ["category_name", "p1", "year", "month", "day", "hour", "value"]
    print(f"Processing collection '{collection_name}' with PeriodEnum {period_enum_value} and sol files: {sol_files}")

    sol = Solution()

    # Iterate through sol_files
    for sol_file in sol_files:
        sol_file_path = os.path.join(input_folder, sol_file)
        print(sol_file_path)
        folder_name = "outputs"
        
        solution_name = os.path.splitext(sol_file)[0]
        solution_output_folder = os.path.join(output_folder, period_enum_value, solution_name, folder_name)
        output_csv_file = os.path.join(solution_output_folder, output_filename)

        # Check if the solution file exists, delete it if it does
        if os.path.exists(output_csv_file):
            os.remove(output_csv_file)  # Deletes the specific solution file

        os.makedirs(solution_output_folder, exist_ok=True)

        sol.Connection(sol_file_path)
        print(f'Processing {collection_name} (ID: {collection_id}) for {sol_file}...')

        try:
            if period_enum_value == "Interval":
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
                        str(property_id),  # Convert property_id to string
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
                # Non-interval case
                print(f'Processing entire horizon for {collection_name} for {sol_file}...')
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
                    str(property_id),  # Convert property_id to string
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
            error_message = f"Error processing {collection_name} for {sol_file_path}: {e}"
            print(error_message)
            error_file = "error_log.txt"
            with open(error_file, "a") as f:
                f.write(f"Error processing {collection_name} for {sol_file_path}: {e}\n")
                f.write(traceback.format_exc() + "\n")
            input('Press any key to continue...')
        finally:
            sol.Close()

def main():
    # Check if mappings.json exists
    if not os.path.exists('mappings.json'):
        print("mappings.json not found. Please run the mapping script to generate mappings.json.")
        input('Press any key to exit...')
        sys.exit()

    # Read mappings.json
    with open('mappings.json', 'r') as f:
        mappings = json.load(f)

    # Parse CollectionEnum to get collection mapping
    collection_enum_str = '''
    CollectionEnum
        SystemGenerators = 1
        GeneratorTemplate = 2
        ListGenerators = 3
        GeneratorHeatInput = 4
        GeneratorTransition = 5
        GeneratorPowerStation = 6
        GeneratorFuels = 7
        GeneratorStartFuels = 8
        GeneratorSourcePower2X = 9
        GeneratorHeadStorage = 10
        GeneratorTailStorage = 11
        GeneratorNodes = 12
        GeneratorNodes_star_ = 13
        GeneratorHeatInputNodes = 14
        GeneratorHeatOutputNodes = 15
        GeneratorSourceGasFields = 16
        GeneratorSourceGasPlants = 17
        GeneratorGasNode = 18
        GeneratorStartGasNodes = 19
        GeneratorSourceGasStorages = 20
        GeneratorSourceGasContracts = 21
        GeneratorSourceGasTransports = 22
        GeneratorWaterNode = 23
        GeneratorCompanies = 24
        GeneratorCommoditiesConsumed = 25
        GeneratorCommoditiesProduced = 26
        GeneratorMaintenances = 27
        GeneratorFlowNodes = 28
        GeneratorCapacityMarkets = 29
        GeneratorHeatMarkets = 30
        GeneratorMarktoMarkets = 31
        GeneratorConstraints = 32
        GeneratorObjectives = 33
        GeneratorDecisionVariables = 34
        GeneratorConditions = 35
        SystemPowerStations = 36
        PowerStationTemplate = 37
        ListPowerStations = 38
        PowerStationNodes = 39
        SystemFuels = 40
        FuelTemplate = 41
        ListFuels = 42
        FuelSourcePower2X = 43
        FuelSourceGasFields = 44
        FuelSourceGasPlants = 45
        FuelGasNodes = 46
        FuelSourceGasStorages = 47
        FuelSourceGasContracts = 48
        FuelSourceGasTransports = 49
        FuelCompanies = 50
        FuelFacilities = 51
        FuelFlowNodes = 52
        FuelMarkets = 53
        FuelConstraints = 54
        FuelObjectives = 55
        FuelConditions = 56
        SystemFuelContracts = 57
        FuelContractTemplate = 58
        ListFuelContracts = 59
        FuelContractGenerators = 60
        FuelContractFuel = 61
        FuelContractCompanies = 62
        FuelContractConstraints = 63
        FuelContractObjectives = 64
        SystemPower2X = 65
        Power2XTemplate = 66
        ListPower2X = 67
        Power2XFuels = 68
        Power2XNodes = 69
        Power2XHeatNodes = 70
        Power2XHeatStorages = 71
        Power2XGasNodes = 72
        Power2XGasStorages = 73
        Power2XWaterNodes = 74
        Power2XCompanies = 75
        Power2XCommodities = 76
        Power2XFlowNodes = 77
        Power2XConstraints = 78
        Power2XObjectives = 79
        SystemBatteries = 80
        BatteryTemplate = 81
        ListBatteries = 82
        BatteryNodes = 83
        BatteryNodes_star_ = 84
        BatteryCompanies = 85
        BatteryCommoditiesConsumed = 86
        BatteryCommoditiesProduced = 87
        BatteryFlowNodes = 88
        BatteryCapacityMarkets = 89
        BatteryConstraints = 90
        BatteryObjectives = 91
        BatteryConditions = 92
        SystemStorages = 93
        StorageTemplate = 94
        ListStorages = 95
        StorageWaterNodes = 96
        StorageConstraints = 97
        StorageObjectives = 98
        StorageConditions = 99
        StorageGlobals = 100
        SystemEmissions = 108
        EmissionGenerators = 111
        SystemRegions = 200
        SystemZones = 237
    '''
    collection_mapping = parse_collection_enum(collection_enum_str)

    # Declare the collections to process from mappings.json
    collections = mappings  # This is a dictionary with collection_ids as keys

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
            print("Please enter 'FiscalYear' or 'Interval' ")
            period_enum_value = input()
            for collection_id_str, properties in collections.items():
                collection_id = int(collection_id_str)
                collection_name = collection_mapping.get(collection_id, f"Collection_{collection_id}")
                for property_id in properties:
                    print(f"Processing property {property_id} for collection {collection_name} (ID: {collection_id})")
                    process_collection_chunk(collection_id, collection_name, input_folder, output_folder, sol_files, property_id, period_enum_value)
            print("Appending '_append' files to corresponding CSVs...")
            append_files(output_folder)
        except Exception as e:
            print(f"Execution failed with error: {e}")

if __name__ == '__main__':
    main()
