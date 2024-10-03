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

def process_collection(collection, input_folder, output_folder, sol_files, property):
    """
    Function to process a collection of data and create specific files based on collection and property.

    Args:
    - collection: The collection name.
    - input_folder: Path to the input folder.
    - output_folder: Path to the output folder.
    - sol_files: List of solution files.
    - property: Property to process.
    """
    import os
    import pandas as pd
    import csv
    from datetime import timedelta
    from dateutil.relativedelta import relativedelta

    # Determine the correct filename and columns based on the collection and property
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
    period_enum_value = "FiscalYear" 
    folder_name = "outputs"
    sol = Solution()
    simulation     = SimulationPhaseEnum.LTPlan
    periodEnum     = getattr(PeriodEnum, f'{period_enum_value}')  
    collectionEnum = int(getattr(CollectionEnum, f'System{collection}') if collection != 'EmissionGenerators' else CollectionEnum.EmissionGenerators)
    parentName     = "" 
    childName      = ""
    seriesEnum     = SeriesTypeEnum.Properties
    # Iterate through sol_files
    for sol_file in sol_files:
        sol_file_path = os.path.join(input_folder, sol_file)
        solution_name = os.path.splitext(sol_file)[0]
        solution_output_folder = os.path.join(output_folder, period_enum_value , solution_name, folder_name)

        os.makedirs(solution_output_folder, exist_ok=True)

        sol.Connection(sol_file_path)
        print(f'Processing {collection} with property {property} for {sol_file}...')

        try:
            output_csv_file = os.path.join(solution_output_folder, output_filename)
            
            result = sol.QueryToList(
                    simulation,       # SimulationPhaseEnum
                    collectionEnum,   # Int32
                    parentName,       # String
                    childName,        # String
                    periodEnum,       # PeriodEnum
                    seriesEnum,       # SeriesTypeEnum
                    property,         # String (property ID)
                )

            with open(output_csv_file, 'w', newline='') as csvfile:
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

        except Exception as e:
            print(f"Error processing {collection} for {sol_file}: {e}")
        finally:
            sol.Close()



# Get collections from the config file
collections_str = "Batteries Emissions Generators"
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
        for collection in collections:
            match collection:
                case "Generators":
                    properties = ["2", "240"]
                case "Batteries":
                    properties = ["5", "6", "82"]
                case "Emissions":
                    properties = ["1"]
                case _:
                    properties = []
            
            for property in properties:
                process_collection(collection, input_folder, output_folder, sol_files, property)
    except Exception as e:
        print(f"Execution failed with error: {e}")
