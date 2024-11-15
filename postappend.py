import os
import pandas as pd

# Define the target directory
target_directory = 'runs'

# Check if the directory exists
if not os.path.exists(target_directory):
    print(f"Directory '{target_directory}' does not exist.")
else:
    # Traverse the 'runs' directory and its subdirectories
    for root, dirs, files in os.walk(target_directory):
        print(f"Checking directory: {root}")  # Debug print to see which directory is being checked
        
        for file in files:
            print(f"Found file: {file}")  # Debug print to see found files
            
            if 'apend' in file:  # Adjusted condition to match your file naming pattern
                print(f"Processing file: {file}")  # Debug print for matching files
                
                # Define the main file name by removing the entire '_apend' part from the current file's name
                main_file_name = file.replace('_apend', '')
                append_file_path = os.path.join(root, file)
                main_file_path = os.path.join(root, main_file_name)
                
                # Check if the main file exists
                if os.path.exists(main_file_path):
                    try:
                        # Load the data from both files
                        append_df = pd.read_csv(append_file_path)
                        main_df = pd.read_csv(main_file_path)
                        
                        # Append the data and save back to the main file
                        combined_df = pd.concat([main_df, append_df], ignore_index=True)
                        combined_df.to_csv(main_file_path, index=False)
                        
                        # Print status
                        print(f"Appended {append_file_path} to {main_file_path}")
                        
                        # Delete the apend file
                        if os.path.exists(append_file_path):
                            print(f"Attempting to delete: {append_file_path}")
                            os.remove(append_file_path)
                            print(f"Deleted {append_file_path}")
                        else:
                            print(f"File {append_file_path} not found when trying to delete.")
                    
                    except Exception as e:
                        print(f"An error occurred while processing {append_file_path}: {e}")
                else:
                    print(f"Main file {main_file_path} not found for {append_file_path}")
