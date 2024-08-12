import pandas as pd
import os
import tkinter as tk
from tkinter import ttk, messagebox

# Define the root input folder and output base folder
input_base_folder = 'PlexosOutputs'
output_base_folder = 'runs'

class CSVProcessorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("CSV Processor")

        self.process_button = tk.Button(root, text="Process Files", command=self.process_files)
        self.process_button.pack(pady=10)

        self.customize_button = tk.Button(root, text="Customize CSV", command=self.open_customize_window)
        self.customize_button.pack(pady=10)

        self.copy_button = tk.Button(root, text="Copy Runs Folder Path", command=self.copy_runs_folder_path)
        self.copy_button.pack(pady=10)

        self.last_processed_file = None  # Variable to store the path of the last processed file
        self.runs_folder_path = os.path.abspath(output_base_folder)  # Store the absolute path of the runs folder

    def process_files(self):
        # Iterate through each scenario subfolder
        for scenario in os.listdir(input_base_folder):
            scenario_path = os.path.join(input_base_folder, scenario)
            
            # Check if it is a directory
            if os.path.isdir(scenario_path):
                print(f'Processing scenario: {scenario}')
                
                # Paths to the CSV files
                generators_file_path = os.path.join(scenario_path, 'Generators.csv')
                emissions_file_path = os.path.join(scenario_path, 'Emissions.csv')
                batteries_file_path = os.path.join(scenario_path, 'Batteries.csv')
                
                if os.path.exists(generators_file_path):
                    self.process_generators_file(scenario, generators_file_path)
                
                if os.path.exists(emissions_file_path):
                    self.process_emissions_file(scenario, emissions_file_path)

                if os.path.exists(batteries_file_path):
                    self.process_batteries_file(scenario, batteries_file_path)

                self.create_cap_csv(scenario, generators_file_path, batteries_file_path)

        messagebox.showinfo("Processing Complete", "All files have been processed successfully.")

    def process_generators_file(self, scenario, file_path):
        df_gen = pd.read_csv(file_path)
        
        # Extract year, month, and hour from the _date column using string slicing
        df_gen['year'] = df_gen['_date'].str.split(' ').str[0].str.split('/').str[2]  # Year part of the date string
        df_gen['hour'] = 'h' + df_gen['_date'].str.split(' ').str[1].str.split(':').str[0]  # Hour part of the time string, prefixed with 'h'
        df_gen['month'] = 'p' + df_gen['_date'].str.split('/').str[0]  # First part of the date string for month, prefixed with 'p'
        
        output_dir = os.path.join(output_base_folder, scenario, 'outputs')
        os.makedirs(output_dir, exist_ok=True)

        # Create and save the gen_h.csv file
        data_gen_h = {
            "Dim1": df_gen["category_name"],
            "Dim2": df_gen["month"],
            "Dim3": df_gen["hour"],
            "Dim4": df_gen["year"],
            "Val": df_gen["Generation (GWh)"]
        }
        df_gen_h = pd.DataFrame(data_gen_h)
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

    def process_batteries_file(self, scenario, file_path):
        df_bat = pd.read_csv(file_path)

        # Extract year from _date column
        if '_date' in df_bat.columns:
            df_bat['year'] = df_bat['_date'].str.split(' ').str[0].str.split('/').str[2]
            df_bat['hour'] = 'h' + df_bat['_date'].str.split(' ').str[1].str.split(':').str[0]  # Hour part of the time string, prefixed with 'h'
            df_bat['month'] = 'p' + df_bat['_date'].str.split('/').str[0]  # First part of the date string for month, prefixed with 'p'

        output_dir = os.path.join(output_base_folder, scenario, 'outputs')
        os.makedirs(output_dir, exist_ok=True)

        # Prepare data for appending to existing gen_h.csv, gen_ivrt.csv, gen_ann.csv
        data_bat_h = {
            "Dim1": df_bat["category_name"],
            "Dim2": df_bat["month"],
            "Dim3": df_bat["hour"],
            "Dim4": df_bat["year"],
            "Val": df_bat["Generation (GWh)"]
        }
        df_bat_h = pd.DataFrame(data_bat_h)

        data_bat_ivrt = {
            "Dim1": df_bat["category_name"],
            "Dim2": df_bat["child_name"],
            "Dim3": 'p1',
            "Dim4": df_bat["year"],
            "Val": df_bat["Generation (GWh)"]
        }
        df_bat_ivrt = pd.DataFrame(data_bat_ivrt)

        data_bat_ann = {
            "Dim1": df_bat["category_name"],
            "Dim2": 'p1',
            "Dim3": df_bat["year"],
            "Val": df_bat["Generation (GWh)"]
        }
        df_bat_ann = pd.DataFrame(data_bat_ann)

        # Paths to existing CSV files
        gen_h_file_path = os.path.join(output_dir, 'gen_h.csv')
        gen_ivrt_file_path = os.path.join(output_dir, 'gen_ivrt.csv')
        gen_ann_file_path = os.path.join(output_dir, 'gen_ann.csv')

        # Append data to existing CSV files or create new ones if they don't exist
        if os.path.exists(gen_h_file_path):
            df_existing_gen_h = pd.read_csv(gen_h_file_path)
            df_combined_gen_h = pd.concat([df_existing_gen_h, df_bat_h], ignore_index=True)
        else:
            df_combined_gen_h = df_bat_h

        if os.path.exists(gen_ivrt_file_path):
            df_existing_gen_ivrt = pd.read_csv(gen_ivrt_file_path)
            df_combined_gen_ivrt = pd.concat([df_existing_gen_ivrt, df_bat_ivrt], ignore_index=True)
        else:
            df_combined_gen_ivrt = df_bat_ivrt

        if os.path.exists(gen_ann_file_path):
            df_existing_gen_ann = pd.read_csv(gen_ann_file_path)
            df_combined_gen_ann = pd.concat([df_existing_gen_ann, df_bat_ann], ignore_index=True)
        else:
            df_combined_gen_ann = df_bat_ann

        # Save the updated CSV files
        df_combined_gen_h.to_csv(gen_h_file_path, index=False)
        print(f'Updated gen_h.csv with batteries data: {gen_h_file_path}')

        df_combined_gen_ivrt.to_csv(gen_ivrt_file_path, index=False)
        print(f'Updated gen_ivrt.csv with batteries data: {gen_ivrt_file_path}')

        df_combined_gen_ann.to_csv(gen_ann_file_path, index=False)
        print(f'Updated gen_ann.csv with batteries data: {gen_ann_file_path}')


    def process_emissions_file(self, scenario, file_path):
        df_emi = pd.read_csv(file_path)
        
        # Extract the year from the _date column
        df_emi['year'] = df_emi['_date'].str.split(' ').str[0].str.split('/').str[2]  # Year part of the date string
        
        output_dir = os.path.join(output_base_folder, scenario, 'outputs')
        os.makedirs(output_dir, exist_ok=True)

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

    def create_cap_csv(self, scenario, generators_file_path, batteries_file_path):
        if not os.path.exists(batteries_file_path):
            messagebox.showwarning("File Error", "Batteries.csv not found.")
            return

        # Load data from Generators and Batteries CSV files
        df_gen = pd.read_csv(generators_file_path)
        df_bat = pd.read_csv(batteries_file_path)

        # Extract year from _date column
        if '_date' in df_gen.columns:
            df_gen['year'] = df_gen['_date'].str.split(' ').str[0].str.split('/').str[2]
        if '_date' in df_bat.columns:
            df_bat['year'] = df_bat['_date'].str.split(' ').str[0].str.split('/').str[2]

        # Prepare data from Generators and Batteries
        df_gen_cap = df_gen[['category_name', 'year', 'Installed Capacity (MW)']].copy()
        df_gen_cap['Dim2'] = 'p1'
        df_gen_cap.rename(columns={'category_name': 'Dim1', 'Installed Capacity (MW)': 'Val'}, inplace=True)
        
        df_bat_cap = df_bat[['category_name', 'year', 'Installed Capacity (MWh)']].copy()
        df_bat_cap['Dim2'] = 'p1'
        df_bat_cap.rename(columns={'category_name': 'Dim1', 'Installed Capacity (MWh)': 'Val'}, inplace=True)

        # Combine the data
        df_combined = pd.concat([df_gen_cap, df_bat_cap], ignore_index=True)
        df_combined['Dim3'] = df_combined['year']
        df_combined = df_combined[['Dim1', 'Dim2', 'Dim3', 'Val']]

        # Save the combined data to cap.csv
        output_dir = os.path.join(output_base_folder, scenario, 'outputs')
        os.makedirs(output_dir, exist_ok=True)
        output_file_path = os.path.join(output_dir, 'cap.csv')
        df_combined.to_csv(output_file_path, index=False)
        print(f'Saved transformed file to: {output_file_path}')

        # Update last processed file path
        self.last_processed_file = os.path.abspath(output_file_path)

    def open_customize_window(self):
        CustomizationWindow(self.root)

    def copy_runs_folder_path(self):
        if self.runs_folder_path:
            self.root.clipboard_clear()
            self.root.clipboard_append(self.runs_folder_path)
            self.root.update()  # Now it stays on the clipboard
            messagebox.showinfo("Copied", f"Runs folder path copied to clipboard: {self.runs_folder_path}")
        else:
            messagebox.showwarning("Error", "Runs folder path is not set.")

class CustomizationWindow:
    def __init__(self, parent):
        self.window = tk.Toplevel(parent)
        self.window.title("Customize CSV")

        self.scenario_var = tk.StringVar()
        self.csv_file_var = tk.StringVar()
        self.output_csv_name = tk.StringVar()
        self.dim_vars = []
        self.dim_name_vars = []
        self.dim_dropdowns = []
        self.val_var = tk.StringVar()

        self.scenario_label = tk.Label(self.window, text="Select Scenario")
        self.scenario_label.grid(row=0, column=0, padx=10, pady=10)

        self.scenario_dropdown = ttk.Combobox(self.window, textvariable=self.scenario_var)
        self.scenario_dropdown['values'] = os.listdir(input_base_folder)
        self.scenario_dropdown.grid(row=0, column=1, padx=10, pady=10)
        self.scenario_dropdown.bind('<<ComboboxSelected>>', self.update_csv_files)

        self.csv_label = tk.Label(self.window, text="Select CSV File")
        self.csv_label.grid(row=1, column=0, padx=10, pady=10)

        self.csv_dropdown = ttk.Combobox(self.window, textvariable=self.csv_file_var)
        self.csv_dropdown.grid(row=1, column=1, padx=10, pady=10)
        self.csv_dropdown.bind('<<ComboboxSelected>>', self.update_column_options)

        self.output_name_label = tk.Label(self.window, text="Output CSV Name")
        self.output_name_label.grid(row=2, column=0, padx=10, pady=10)

        self.output_name_entry = tk.Entry(self.window, textvariable=self.output_csv_name)
        self.output_name_entry.grid(row=2, column=1, padx=10, pady=10)

        self.add_dim_button = tk.Button(self.window, text="Add Dimension", command=self.add_dimension)
        self.add_dim_button.grid(row=3, column=0, padx=10, pady=10)

        self.remove_dim_button = tk.Button(self.window, text="Remove Dimension", command=self.remove_dimension)
        self.remove_dim_button.grid(row=3, column=1, padx=10, pady=10)

        self.val_label = tk.Label(self.window, text="Value Column")
        self.val_label.grid(row=4, column=0, padx=10, pady=10)

        self.val_dropdown = ttk.Combobox(self.window, textvariable=self.val_var)
        self.val_dropdown.grid(row=4, column=1, padx=10, pady=10)

        self.process_button = tk.Button(self.window, text="Process Custom CSV", command=self.process_custom_csv)
        self.process_button.grid(row=5, column=0, columnspan=2, padx=10, pady=10)

    def update_csv_files(self, event):
        scenario = self.scenario_var.get()
        scenario_path = os.path.join(input_base_folder, scenario)
        csv_files = [f for f in os.listdir(scenario_path) if f.endswith('.csv')]
        self.csv_dropdown['values'] = csv_files

    def update_column_options(self, event):
        scenario = self.scenario_var.get()
        csv_file = self.csv_file_var.get()
        csv_path = os.path.join(input_base_folder, scenario, csv_file)
        df = pd.read_csv(csv_path)
        columns = df.columns.tolist()
        for dim_var in self.dim_vars:
            dim_var.set('')
        for dim_dropdown in self.dim_dropdowns:
            dim_dropdown['values'] = columns
        self.val_var.set('')
        self.val_dropdown['values'] = columns

    def add_dimension(self):
        dim_index = len(self.dim_vars) + 1
        dim_name = f"Dim{dim_index}"
        dim_name_var = tk.StringVar(value=dim_name)
        dim_var = tk.StringVar()
        self.dim_name_vars.append(dim_name_var)
        self.dim_vars.append(dim_var)

        row_index = len(self.dim_vars) + 4

        dim_label = tk.Label(self.window, textvariable=dim_name_var)
        dim_label.grid(row=row_index, column=0, padx=10, pady=5)

        dim_dropdown = ttk.Combobox(self.window, textvariable=dim_var)
        dim_dropdown.grid(row=row_index, column=1, padx=10, pady=5)

        if self.csv_file_var.get():
            scenario = self.scenario_var.get()
            csv_file = self.csv_file_var.get()
            csv_path = os.path.join(input_base_folder, scenario, csv_file)
            df = pd.read_csv(csv_path)
            columns = df.columns.tolist()
            dim_dropdown['values'] = columns

        self.dim_dropdowns.append(dim_dropdown)

        # Move the value and process button down
        self.val_label.grid(row=row_index + 1, column=0, padx=10, pady=10)
        self.val_dropdown.grid(row=row_index + 1, column=1, padx=10, pady=10)
        self.process_button.grid(row=row_index + 2, column=0, columnspan=2, padx=10, pady=10)

    def remove_dimension(self):
        if self.dim_vars:
            dim_name_var = self.dim_name_vars.pop()
            dim_var = self.dim_vars.pop()
            dim_dropdown = self.dim_dropdowns.pop()

            row_index = len(self.dim_vars) + 5
            for widget in self.window.grid_slaves(row=row_index, column=0):
                widget.grid_forget()
            for widget in self.window.grid_slaves(row=row_index, column=1):
                widget.grid_forget()

            # Move the value and process button up
            self.val_label.grid(row=row_index, column=0, padx=10, pady=10)
            self.val_dropdown.grid(row=row_index, column=1, padx=10, pady=10)
            self.process_button.grid(row=row_index + 1, column=0, columnspan=2, padx=10, pady=10)

    def process_custom_csv(self):
        scenario = self.scenario_var.get()
        csv_file = self.csv_file_var.get()
        output_name = self.output_csv_name.get()

        if not scenario or not csv_file or not output_name:
            messagebox.showwarning("Input Error", "Please fill in all fields.")
            return

        csv_path = os.path.join(input_base_folder, scenario, csv_file)
        df = pd.read_csv(csv_path)

        data = {}
        for i, (dim_var, dim_name_var) in enumerate(zip(self.dim_vars, self.dim_name_vars)):
            column = dim_var.get()
            dim_name = dim_name_var.get()
            if not column or not dim_name:
                messagebox.showwarning("Input Error", f"Dimension {i + 1} or its name is not set.")
                return
            data[dim_name] = df[column]

        val_column = self.val_var.get()
        if not val_column:
            messagebox.showwarning("Input Error", "Value column is not set.")
            return
        data["Val"] = df[val_column]

        output_df = pd.DataFrame(data)
        
        # Ensure the value column is the last one
        columns_order = [dim_name_var.get() for dim_name_var in self.dim_name_vars] + ["Val"]
        output_df = output_df[columns_order]

        output_dir = os.path.join(output_base_folder, scenario, 'outputs')
        os.makedirs(output_dir, exist_ok=True)
        output_file_path = os.path.join(output_dir, output_name)
        output_df.to_csv(output_file_path, index=False)
        messagebox.showinfo("Success", f"Custom CSV saved to: {output_file_path}")

if __name__ == "__main__":
    root = tk.Tk()
    app = CSVProcessorApp(root)
    root.mainloop()
