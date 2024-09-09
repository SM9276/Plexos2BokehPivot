# PLEXOS2BokehPivot

Welcome to PLEXOS2BokehPivot! This project converts PLEXOS XML solution files to CSV format and visualizes the data using Bokeh Pivot. Follow these steps to set up, convert, and visualize your data.

## Installation

### Set Up the XML to CSV Environment

1. Locate the `setup.bat` file in the root directory.
2. Double-click `setup.bat` to run it. This script will install the XML to CSV environment necessary for the project.
3. Place your PLEXOS solution files into the `PlexosSolutions` directory.

### Set Up the Bokeh Pivot Environment

1. Navigate to the `X2BokehPivot` folder.
2. Run `setup.bat` to install the Bokeh Pivot environment.
3. Launch Bokeh Pivot by running `launch.bat`.

## Running the Program

### Convert PLEXOS Solution to CSV

1. **Prepare Your PLEXOS Solution Files:**
   - Ensure that all your PLEXOS solution files are placed in the `PlexosSolutions` directory, organized by scenario.

2. **Check Configuration:**
   - Open `config.csv` to verify that all parameters are correct.

3. **Run the program**
   - The output CSV files will be saved in the `runs` directory.
   - Each scenario will have its own directory within `runs`, containing the processed files.

   **Note:** The script processes LTPlans by default. To switch to STSchedule, update the following lines in the script:

    ```python
    SimulationPhaseEnum.LTPlan
    ```

    to

    ```python
    SimulationPhaseEnum.STSchedule
    ```

### Convert PLEXOS CSV to ReEDS CSV

1. **Launch Plexos2BokehPivot Mapping Tool:**
   - Double-click `setting.bat` to start the tool.
   - Select "Mapping mode" when prompted.

2. **Map Your Columns:**
   - The tool will list CSV files from the `PlexosOutputs` folder. Choose the file you want to map, such as `generation.csv`.
   - Map the columns to the dimensions required by Bokeh Pivot:
     - **Example Mapping:**
       - If you have a column named "category_name," map it to `Dim1`.
       - For fixed values (e.g., a constant region), type `constant` and enter the value.
       - Select the column for `Val` as the value column.

3. **Save Your Configuration:**
   - Enter a name for your mapping configuration to easily identify it later.
   - The tool will automatically save your mapping settings in `configuration.json`.

4. **Generate Output Files:**
   - Run the tool again, choosing "Execute mode."
   - The tool will generate new CSV files in the `runs` folder, named according to your mapping configuration.

## Visualizing the Data with Bokeh Pivot

1. **Open Bokeh Pivot:**
   - Navigate to the `X2BokehPivot` directory.
   - Run `launch.bat` to start Bokeh Pivot.
   - When the browser opens, the path to the `runs` directory is selected
   - remove a letter press enter and put the letter back and press enter

2. **Load and Visualize Your Data:**
   - Apply the visualization you selected to see your data represented effectively.
   - By default the program selects the last year, month and day for hourly data.
   - By default the program selects the last year, month for daily data.
   - By default the program selects the last year, for monthly data. 

Note that data will can only show data correctly if only 1 year, 1 month, 1 day is selcected since the hourly data will be concatnated.
Same applies for the daily data, in this case its only 1 year and 1 month. Same aplies to monthly data, only one year at a time unless you 'explode' by year.



3. **Customize Visualizations:**
   - To customize the colors of technologies, go to `X2BokehPivot/in/reeds2` and modify the `tech_style.csv` file.

## Example Workflow

1. **Prepare Your Files:**
   - Place your PLEXOS solution files into the `PlexosSolutions` directory.

2. **Run the program:**
   - Verify configuration in `config.csv`.
   - Run `lanuch.bat` to generate CSV files

3. **Visualize with Bokeh Pivot:**
   - Start Bokeh Pivot and paste the path to the `runs` directory.
   - Import the CSV files and apply the desired visualizations.
   - Customize the visualizations as needed.
