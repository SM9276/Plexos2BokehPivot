import os
import pandas as pd
import json
import re

CONFIG_FILE = 'configurations.json'
INPUT_FOLDER = 'PlexosOutputs'
OUTPUT_FOLDER = 'CSV'

def load_configurations():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as file:
            return json.load(file)
    return {}

def save_configurations(configurations):
    with open(CONFIG_FILE, 'w') as file:
        json.dump(configurations, file, indent=4)

def list_all_csv_files(folder):
    """ List all unique CSV files with their paths. """
    csv_files = {}
    for root, _, files in os.walk(folder):
        for file in files:
            if file.endswith('.csv'):
                relative_path = os.path.relpath(os.path.join(root, file), folder)
                csv_files[file] = os.path.join(root, file)
    return csv_files

def list_all_csv_files_with_repeats(folder):
    """ List all CSV files with their full paths, including duplicates. """
    csv_files = []
    for root, _, files in os.walk(folder):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    return csv_files

def map_columns(old_csv):
    df = pd.read_csv(old_csv)
    return df.columns.tolist()

def mapping_mode():
    configurations = load_configurations()
    csv_files = list_all_csv_files(INPUT_FOLDER)
    
    if not csv_files:
        print("No CSV files found in the 'inputs' folder.")
        return

    print("Available CSV files (names only):")
    unique_names = set(os.path.basename(path) for path in csv_files.values())
    for idx, file_name in enumerate(unique_names):
        print(f"{idx + 1}. {file_name}")
    
    file_idx = int(input("Select a CSV file by number: ")) - 1
    selected_file_name = list(unique_names)[file_idx]
    old_csv_path = csv_files[selected_file_name]
    columns = map_columns(old_csv_path)
    
    print("Available columns to map:")
    for idx, col in enumerate(columns):
        print(f"{idx + 1}. {col}")
    
    dimensions = {}
    dimension_count = 1
    
    while True:
        print(f"Enter column for dimension {dimension_count} (or type 'value' to select value column, or type 'constant' to enter a constant string):")
        user_input = input()
        
        if user_input.lower() == 'constant':
            constant_value = input(f"Enter constant value for dimension {dimension_count}: ")
            dimensions[f"Dim{dimension_count}"] = constant_value
            dimension_count += 1
        elif user_input.lower() == 'value':
            break
        else:
            try:
                dim_col = int(user_input) - 1
                if 0 <= dim_col < len(columns):
                    dimensions[f"Dim{dimension_count}"] = columns[dim_col]
                    dimension_count += 1
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number, 'constant', or 'value'.")
    
    print("Available columns for value:")
    for idx, col in enumerate(columns):
        print(f"{idx + 1}. {col}")
    
    value_column = int(input("Select column for values by number: ")) - 1
    if 0 <= value_column < len(columns):
        dimensions["Val"] = columns[value_column]
    else:
        print("Invalid selection. No value column selected.")
        return
    
    new_file_name = input("Enter the new file name (without extension): ")
    if not new_file_name:
        print("Invalid file name. Configuration not saved.")
        return
    
    configurations[new_file_name] = {
        "original_file": selected_file_name,
        "dimensions": dimensions
    }
    
    save_configurations(configurations)
    print("Configuration saved.")

def execute_mode():
    configurations = load_configurations()
    all_csv_files = list_all_csv_files_with_repeats(INPUT_FOLDER)
    
    for full_path in all_csv_files:
        file_name = os.path.basename(full_path)
        
        # Iterate over all configurations for the current file
        for new_file_name, config in configurations.items():
            if config["original_file"] == file_name:
                df = pd.read_csv(full_path)
                new_df = pd.DataFrame()
                dimensions = config["dimensions"]
                
                # Add dimensions to new DataFrame
                for key in dimensions:
                    if key.startswith("Dim"):
                        column_name = dimensions[key]
                        if column_name in df.columns:
                            new_df[key] = df[column_name]
                        else:
                            # Handle constant string
                            new_df[key] = [column_name] * len(df)

                # Add value column to new DataFrame
                if "Val" in dimensions:
                    value_col = dimensions["Val"]
                    
                    # Extract the term before parentheses if present
                    col_name_before_parens = re.split(r'\s*\(', value_col)[0].strip()
                    
                    # Find the matching column in the CSV based on the extracted term
                    matching_columns = [col for col in df.columns if col_name_before_parens in col]
                    if matching_columns:
                        # Use the first matching column in the list
                        new_df['Val'] = df[matching_columns[0]]
                    else:
                        print(f"Warning: No matching column found for '{value_col}' in {file_name}.")
                        continue

                # Determine the output folder structure (recreate full path structure)
                relative_folder = os.path.relpath(os.path.dirname(full_path), INPUT_FOLDER)
                output_folder = os.path.join(OUTPUT_FOLDER, relative_folder)
                
                # Create the new CSV file
                new_csv = os.path.join(output_folder, f"{new_file_name}.csv")
                os.makedirs(os.path.dirname(new_csv), exist_ok=True)
                new_df.to_csv(new_csv, index=False)
                print(f"New CSV file created: {new_csv}")

        # Handle cases where there is no configuration for the current file
        if not any(config["original_file"] == file_name for config in configurations.values()):
            print(f"Warning: Configuration for '{file_name}' not found in the configurations.")

def main():
    mode = input("Enter mode (Mapping/Execute): ").strip().lower()
    if mode == 'mapping':
        mapping_mode()
    elif mode == 'execute':
        execute_mode()
    else:
        print("Invalid mode.")

if __name__ == "__main__":
    main()
