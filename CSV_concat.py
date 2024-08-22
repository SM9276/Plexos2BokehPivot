import os
import pandas as pd

def concatenate_csvs_in_subfolders(root_folder):
    # Create the base 'runs' folder outside of the input folder
    runs_folder = os.path.join(os.path.dirname(root_folder), 'runs')
    os.makedirs(runs_folder, exist_ok=True)

    # Walk through the root folder and its subfolders
    for subdir, _, files in os.walk(root_folder):
        dataframes = {}
        
        # Get the relative path of the current subdirectory
        relative_path = os.path.relpath(subdir, root_folder)
        # Create a corresponding subdirectory in the 'runs' folder
        output_subdir = os.path.join(runs_folder, relative_path)
        os.makedirs(output_subdir, exist_ok=True)

        for file in files:
            # Check if the file is a CSV
            if file.endswith('.csv'):
                file_path = os.path.join(subdir, file)
                # Extract the name without the extension
                name, _ = os.path.splitext(file)

                # Check for the _append suffix
                if name.endswith('_append'):
                    base_name = name[:-7]  # Remove the '_append' part
                else:
                    base_name = name

                # Read the CSV file into a DataFrame
                df = pd.read_csv(file_path)

                # Append to the appropriate DataFrame in the dictionary
                if base_name not in dataframes:
                    dataframes[base_name] = df
                else:
                    dataframes[base_name] = pd.concat([dataframes[base_name], df], ignore_index=True)

        # Save concatenated DataFrames back to CSV in the corresponding 'runs' subdirectory
        for name, df in dataframes.items():
            output_file = os.path.join(output_subdir, f"{name}.csv")
            df.to_csv(output_file, index=False)
            print(f"Saved: {output_file}")

if __name__ == "__main__":
    concatenate_csvs_in_subfolders('CSV')
    print("CSV files concatenated successfully.")
