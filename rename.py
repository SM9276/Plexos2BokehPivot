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

            # If the file is an append file, append it to the main file
            if '_apend' in new_filename:
                main_file = new_filename.replace('_apend', '')
                main_path = os.path.join(root, main_file)
                if os.path.exists(main_path):
                    # Read and append data
                    df_main = pd.read_csv(main_path)
                    df_append = pd.read_csv(original_path)
                    df_combined = pd.concat([df_main, df_append])
                    df_combined.to_csv(main_path, index=False)
                    print(f"Appended {file} to {main_file}")
                else:
                    # Rename if the main file doesn't exist
                    os.rename(original_path, new_path)
                    print(f"Renamed {file} to {new_filename}")
            else:
                # Rename the file if it doesn't need to be appended
                os.rename(original_path, new_path)
                print(f"Renamed {file} to {new_filename}")
