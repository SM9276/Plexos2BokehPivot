import clr
import sys
import csv

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

def get_available_properties(collection):
    """
    Retrieve and print all available properties for a specific collection along with their enums.

    Args:
    - collection: The collection name (e.g., "Generators", "Batteries").

    Returns:
    - A dictionary of property names and their enums.
    """
    sol = Solution()
    properties_with_enums = {}
    
    try:
        # Connect to the solution file
        sol.Connection(r"C:\Users\alejandro.elenes\Documents\GitHub\Plexos2BokehPivot\PlexosSolutions\Model RPS_000_SPS_000 Solution.zip")  
        properties = sol.GetReportedProperties()
        properties_list = [str(prop) for prop in properties]  # Converts each .NET string to a native Python string
        # properties_list = list(properties)
        print(properties_list[:5])  # Inspect the first few elements for patterns
        exit()
        
        #save list of properties to a csv
        with open('properties.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(["Property"])
            for property_name in properties_list:
                csvwriter.writerow([property_name])

        print(f"Properties and Enums for collection '{collection}':")
        
        for prop in properties_list:
            try:
                # Obtain enum for the property
                enum_id = sol.PropertyName2EnumId("System", "Emission", "EmissionGenerators", prop)
                enum_id = str(enum_id) 
                properties_with_enums[prop] = enum_id
                print(f"{prop}: Enum ID = {enum_id}")
                # is enum_id string or int?
                print(type(enum_id))
            # skip if property is not found
            except Exception as e:
                print(f"Could not retrieve enum for property '{prop}': {e}")

    except Exception as e:
        print(f"Error retrieving properties for collection {collection}: {e}")
    finally:
        sol.Close()
    
    return properties_with_enums


# get properties for a specific collection
get_available_properties("Generators")
