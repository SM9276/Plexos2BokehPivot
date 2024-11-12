import os
import sys
import clr
import pandas as pd

sys.path.append('C:/Program Files/Energy Exemplar/PLEXOS 10.0 API')
clr.AddReference('PLEXOS_NET.Core')
clr.AddReference('EEUTILITY')
clr.AddReference('EnergyExemplar.PLEXOS.Utility')

from PLEXOS_NET.Core import DatabaseCore
from EnergyExemplar.PLEXOS.Utility.Enums import *
from EEUTILITY.Enums import *

def recordset_to_list(rs):
    result = []
    while not rs.EOF:
        row = [rs[f.Name] for f in rs.Fields]
        result.append(row)
        rs.MoveNext()
    return result

def get_report_properties(model_name):
    """
    Get a dictionary mapping collections to their associated properties based on the specified model's reports.
    
    Args:
    - model_name: The name of the model to retrieve report properties for.
    
    Returns:
    - dict: A dictionary where keys are collection names and values are lists of properties.
    """
    #Load the XML model file
    model = DatabaseCore()
    model.Connection(os.path.join(os.path.dirname(__file__), 'testmodel.xml'))
    
    #Extract class and collection IDs
    classes = model.FetchAllClassIds()
    collections = model.FetchAllCollectionIds()

    #Extract Report IDs
    report_names = model.GetChildMembers(collections["ModelReport"], model_name)
    report_ids = [model.ObjectName2Id(classes["Report"], name) for name in report_names]
    
    #Use the GetData function to get a record set with t_report into a pd data frame
    recordset, _ = model.GetData('t_report', [])
    report_data = recordset_to_list(recordset)
    report_df = pd.DataFrame(report_data, columns=[f.Name for f in recordset.Fields])
    recordset.Close()
    
    # Filter to include only reports matching the model
    report_df['filtered_report'] = report_df['object_id'].apply(lambda x: x in report_ids)
    report_df = report_df[report_df['object_id'].isin(report_ids)]
    
    #Use the GetData function to get a record set with t_property_report into a pd data frame
    recordset, _ = model.GetData('t_property_report', [])
    report_prop_data = recordset_to_list(recordset)
    report_prop_df = pd.DataFrame(report_prop_data, columns=[f.Name for f in recordset.Fields])
    recordset.Close()

    # save report_prop_df to a csv file
    # report_prop_df.to_csv(os.path.join(os.path.dirname(__file__), 'report_prop_df.csv'), index=False)
    # exit()
    
    report_prop_df = report_prop_df.set_index('property_id')
    combined_df = report_df.join(report_prop_df,on='property_id',rsuffix='_prop')
    # Join the report df and the property df to associate reports with properties
    #combined_df = report_df.merge(report_prop_df, left_on='object_id', right_on='report_id', suffixes=('', '_prop'))
    
    # Create dictionary mapping each collection to its properties
    #collection_property_map = {}
    #for _, row in combined_df.iterrows():
    #    collection_name = row['collection_name']  # Replace with the actual column name in `t_report`/`t_property_report`
    #    property_name = row['property_name']  # Replace with the actual column name in `t_property_report`
    #    if collection_name not in collection_property_map:
    #        collection_property_map[collection_name] = []
    #    collection_property_map[collection_name].append(property_name)

    #Close the model file and return the property map
    model.Close()
    return combined_df

def main():
    model_name = 'RPS_000_SPS_000'
    get_report_properties(model_name).to_csv(os.path.join(os.path.dirname(__file__),'testmodel.csv'), index=False)
  
    #collection_property_map = get_report_properties(model_name)
    
    # Save the map to a CSV for inspection if needed
    #output_path = os.path.join(os.path.dirname(__file__), 'collection_property_map.csv')
    #pd.DataFrame([
    #    {"Collection": k, "Properties": ', '.join(v)} for k, v in collection_property_map.items()
    #]).to_csv(output_path, index=False)
    
    #print(f"Collection to Property mapping saved at {output_path}")

if __name__ == '__main__':
    main()

    
     
     

    
    
    

    
    

