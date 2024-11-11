import os
import sys
import clr
import pandas as pd

# Set up paths to PLEXOS API libraries
sys.path.append('C:/Program Files/Energy Exemplar/PLEXOS 10.0 API')
clr.AddReference('PLEXOS_NET.Core')
clr.AddReference('EEUTILITY')
clr.AddReference('EnergyExemplar.PLEXOS.Utility')

# Import PLEXOS and required .NET modules
from PLEXOS_NET.Core import DatabaseCore
from EnergyExemplar.PLEXOS.Utility.Enums import *
from EEUTILITY.Enums import *

def recordset_to_list(rs):
    """Convert Recordset to a list of lists."""
    result = []
    while not rs.EOF:
        row = [rs[f.Name] for f in rs.Fields]
        result.append(row)
        rs.MoveNext()
    return result

def get_report_properties(model_name):
    """
    Retrieve a dictionary mapping each collection to its reported properties and enums.

    Args:
    - model_name (str): The model name to retrieve report properties for.

    Returns:
    - dict: A nested dictionary {collection_name: {property_name: enum_id}}
    """
    plx = DatabaseCore()
    plx.Connection(os.path.join(os.path.dirname(__file__), 'rts_PLEXOS.xml'))
    
    # Fetch class and collection IDs
    classes = plx.FetchAllClassIds()
    collections = plx.FetchAllCollectionIds()

    # Retrieve the Report IDs associated with the specified model
    report_names = plx.GetChildMembers(collections["ModelReport"], model_name)
    report_ids = [plx.ObjectName2Id(classes["Report"], name) for name in report_names]

    # Query report data to find reports that match the model
    rst, _ = plx.GetData('t_report', [])
    report_data = recordset_to_list(rst)
    report_df = pd.DataFrame(report_data, columns=[f.Name for f in rst.Fields])
    rst.Close()
    
    # Filter by the selected report IDs
    report_df = report_df[report_df['object_id'].isin(report_ids)]
    
    # Query properties from `t_property_report` to map them to collections
    rst, _ = plx.GetData('t_property_report', [])
    report_prop_data = recordset_to_list(rst)
    report_prop_df = pd.DataFrame(report_prop_data, columns=[f.Name for f in rst.Fields])
    rst.Close()
    
    # Ensure that 'collection_id' is retained for the join
    if 'property_id' not in report_prop_df.columns or 'property_id' not in report_df.columns:
        raise ValueError("Expected columns 'property_id' are missing for merge.")

    # Join on 'property_id' and ensure 'collection_id' is part of the resulting DataFrame
    full_df = report_df.merge(report_prop_df, on='property_id', suffixes=('', '_prop'))

    # Create dictionary mapping each collection to its properties and enums
    collection_property_map = {}
    for _, row in full_df.iterrows():
        collection_name = row.get('collection_id', '')  # Ensure collection ID is mapped correctly
        property_name = row['name']  # Property name in the final report property DataFrame
        enum_id = row['enum_id']  # Enum ID from the property report

        # Initialize dictionary structure
        if collection_name not in collection_property_map:
            collection_property_map[collection_name] = {}
        collection_property_map[collection_name][property_name] = enum_id

    plx.Close()
    return collection_property_map

def main():
    model_name = 'Q3 DA'  # Specify model name
    collection_property_map = get_report_properties(model_name)
    
    # Save the map to CSV for verification if needed
    output_path = os.path.join(os.path.dirname(__file__), 'collection_property_map.csv')
    pd.DataFrame([
        {"Collection": k, "Property": prop, "Enum_ID": enum_id} 
        for k, v in collection_property_map.items() 
        for prop, enum_id in v.items()
    ]).to_csv(output_path, index=False)
    
    print(f"Collection to Property mapping saved at {output_path}")

if __name__ == '__main__':
    main()
