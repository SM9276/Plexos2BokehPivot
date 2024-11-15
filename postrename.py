import os
import pandas as pd

# Define mapping of filenames to their new names
filename_mapping = {
    'collection_1_property_2.csv': 'gen_ann.csv',
    'collection_1_property_214.csv': 'cap.csv',
    'collection_80_property_5.csv': 'gen_ann_apend.csv',
    'collection_80_property_6.csv': 'load.csv',
    'collection_80_property_69.csv': 'cap_apend.csv',
    'collection_111_property_3.csv': 'emit_r.csv'
}

# Traverse the directory and subdirectories
for root, dirs, files in os.walk('.'):
    for file in files:
        if file in filename_mapping:
            # Get the full path of the original file
            original_path = os.path.join(root, file)
            new_filename = filename_mapping[file]
            new_path = os.path.join(root, new_filename)
            
            # Rename the file
            os.rename(original_path, new_path)
            print(f"Renamed {original_path} to {new_path}")
