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
        
        # Paths to the CSV files
        generators_file_path = os.path.join(scenario_path, 'Generators.csv')
        emissions_file_path = os.path.join(scenario_path, 'Emissions.csv')
        
        if os.path.exists(generators_file_path):
            # Load the Generators.csv file
            df_gen = pd.read_csv(generators_file_path)
            
            # Extract year, month, and hour from the _date column using string slicing
            df_gen['year'] = df_gen['_date'].str.split(' ').str[0].str.split('/').str[2]  # Year part of the date string
            df_gen['hour'] = 'h' + df_gen['_date'].str.split(' ').str[1].str.split(':').str[0]  # Hour part of the time string, prefixed with 'h'
            df_gen['month'] = 'p' + df_gen['_date'].str.split('/').str[0]  # First part of the date string for month, prefixed with 'p'
            
            # Create and save the gen_h.csv file
            data_gen_h = {
                "Dim1": df_gen["category_name"],
                "Dim2": df_gen["month"],
                "Dim3": df_gen["hour"],
                "Dim4": df_gen["year"],
                "Val": df_gen["Generation (GWh)"]
            }
            df_gen_h = pd.DataFrame(data_gen_h)
            output_dir = os.path.join(output_base_folder, scenario, 'outputs')
            os.makedirs(output_dir, exist_ok=True)
            output_file_path_gen_h = os.path.join(output_dir, 'gen_h.csv')
            df_gen_h.to_csv(output_file_path_gen_h, index=False)
            print(f'Saved transformed file to: {output_file_path_gen_h}')
            
            # Create and save the gen_ivrt.csv file
            data_gen_ivrt = {
                "Dim1": df_gen["category_name"],
                "Dim2": df_gen["child_name"],
                "Dim3": 'p1',
                "Dim4": df_gen["year"],
                "Val": df_gen["Generation (GWh)"]
            }
            df_gen_ivrt = pd.DataFrame(data_gen_ivrt)
            output_file_path_gen_ivrt = os.path.join(output_dir, 'gen_ivrt.csv')
            df_gen_ivrt.to_csv(output_file_path_gen_ivrt, index=False)
            print(f'Saved transformed file to: {output_file_path_gen_ivrt}')
            
            # Create and save the gen_ann.csv file
            data_gen_ann = {
                "Dim1": df_gen["category_name"],
                "Dim2": 'p1',
                "Dim3": df_gen["year"],
                "Val": df_gen["Generation (GWh)"]
            }
            df_gen_ann = pd.DataFrame(data_gen_ann)
            output_file_path_gen_ann = os.path.join(output_dir, 'gen_ann.csv')
            df_gen_ann.to_csv(output_file_path_gen_ann, index=False)
            print(f'Saved transformed file to: {output_file_path_gen_ann}')
        
        if os.path.exists(emissions_file_path):
            # Load the Emissions.csv file
            df_emi = pd.read_csv(emissions_file_path)
            
            # Extract the year from the _date column
            df_emi['year'] = df_emi['_date'].str.split(' ').str[0].str.split('/').str[2]  # Year part of the date string
            
            # Create and save the emit_r.csv file
            data_emit_r = {
                "Dim1": df_emi["category_name"],
                "Dim2": 'p1',
                "Dim3": df_emi["year"],
                "Val": df_emi["Production (ton)"]
            }
            df_emit_r = pd.DataFrame(data_emit_r)
            output_file_path_emit_r = os.path.join(output_dir, 'emit_r.csv')
            df_emit_r.to_csv(output_file_path_emit_r, index=False)
            print(f'Saved transformed file to: {output_file_path_emit_r}')

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