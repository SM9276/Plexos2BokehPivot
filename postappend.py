import os
import pandas as pd

# Traverse the directory and subdirectories
for root, dirs, files in os.walk('.'):
    for file in files:
        if '_append' in file:
            # Define the main file name by removing '_append' from the current file's name
            main_file_name = file.replace('_append', '')
            append_file_path = os.path.join(root, file)
            main_file_path = os.path.join(root, main_file_name)
            
            # Check if the main file exists
            if os.path.exists(main_file_path):
                # Load the data from both files
                append_df = pd.read_csv(append_file_path)
                main_df = pd.read_csv(main_file_path)
                
                # Append the data and save back to the main file
                combined_df = pd.concat([main_df, append_df], ignore_index=True)
                combined_df.to_csv(main_file_path, index=False)
                
                # Print status
                print(f"Appended {append_file_path} to {main_file_path}")
                
                # Delete the _append file
                os.remove(append_file_path)
                print(f"Deleted {append_file_path}")
            else:
                print(f"Main file {main_file_path} not found for {append_file_path}")
