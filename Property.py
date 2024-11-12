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
    #Load the XML solution file
    solution = DatabaseCore()
    solution.Connection(os.path.join(os.path.dirname(__file__), 'rts_PLEXOS.xml'))
    
    classes = solution.FetchAllClassIDS()
