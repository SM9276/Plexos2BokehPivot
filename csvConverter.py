import pandas as pd
import os

# Define the root input folder and output base folder
input_base_folder = 'PlexosOutputs'
output_base_folder = 'runs'

# Iterate through each scenario subfolder
for scenario in os.listdir(input_base_folder):
    scenario_path = os.path.join(input_base_folder, scenario)
    
    # Check if it is a directory
    if os.path.isdir(scenario_path):
        print(f'Processing scenario: {scenario}')
        
        # Iterate through each CSV file in the scenario subfolder
        for file_name in os.listdir(scenario_path):
            if file_name.endswith('.csv'):
                file_path = os.path.join(scenario_path, file_name)
                print(f'Processing file: {file_path}')
                
                # Load the CSV file
                df1 = pd.read_csv(file_path)
                
                # Extract the year, month, and hour from the _date column using string slicing
                df1['year'] = df1['_date'].str.split(' ').str[0].str.split('/').str[2]  # Year part of the date string
                df1['hour'] = 'h' + df1['_date'].str.split('/').str[0]  # First part of the date string for month, prefixed with 'h'
                df1['month'] = 'p' + df1['_date'].str.split(' ').str[1].str.split(':').str[0]  # Hour part of the time string, prefixed with 'p'
                
                # Create a new DataFrame with the desired structure
                data = {
                    "Dim1": df1["category_name"],
                    "Dim2": df1["month"],
                    "Dim3": df1["hour"],
                    "Dim4": df1["year"],
                    "Val": df1["Generation (GWh)"]
                }
                
                df2 = pd.DataFrame(data)
                
                # Define output directory and file path
                output_dir = os.path.join(output_base_folder, scenario, 'outputs')
                os.makedirs(output_dir, exist_ok=True)
                
                # Save the new DataFrame to a CSV file
                output_file_path = os.path.join(output_dir, file_name)
                df2.to_csv(output_file_path, index=False)
                print(f'Saved transformed file to: {output_file_path}')

"""
import pandas as pd

# Load the first CSV file
df1 = pd.read_csv('Generators.csv')

# Extract the year, month, and hour from the _date column using string slicing
df1['year'] = df1['_date'].str.split(' ').str[0].str.split('/').str[2]  # Year part of the date string
df1['hour'] = 'h' + df1['_date'].str.split('/').str[0]  # First part of the date string for month, prefixed with 'p'
df1['month'] = 'p' + df1['_date'].str.split(' ').str[1].str.split(':').str[0]  # Hour part of the time string, prefixed with 'h'

# Create a new DataFrame with the desired structure
data = {
    "Dim1": df1["category_name"],
    "Dim2": df1["month"],
    "Dim3": df1["hour"],
    "Dim4": df1["year"],
    "Val": df1["Generation (GWh)"]
}

df2 = pd.DataFrame(data)

# Save the new DataFrame to a CSV file
df2.to_csv('gen_h.csv', index=False)
"""