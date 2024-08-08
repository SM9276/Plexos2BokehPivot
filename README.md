# PLEXOS2BokehPivot

## 1) Getting started
Two Python environments are used in this project, one to interact with the PLEXOS API and another to interact with ReEDS Bokeh Pivot. 

The enviorment.yml file found in the root directory is to be used with XML2CSV.py and Plexos2BokehPivotGUI.py. Also make sure to have the PLEXOS API installed.

 Another environment.yml file will be found in the X2BokehPivot directory, this is to be used with Bokeh Pivot.

 ## 2) Starting the program

### PLEXOS Solution to CSV 
Once the PLEXOS Solution files are gathered, place them into the PlexosSolutions directory. Take a look at config.csv, make sure to have all the correct parameters. Select the correct environment and run XML2CSV.py. In the PlexosOutputs directory, each Scenario will have its own dedicated directory with the collections that were processed. 

Note: In the current state of the script, it only processes LTPlans since it was hard coded, but the following lines can change it in lines 174 and 198 to switch it to STSchedule.

    SimulationPhaseEnum.LTPlan
Must be changed to 

    SimulationPhaseEnum.STSchedule

### PLEXOS CSV to ReEDS CSV
Next, run Plexos2BokehPivotGUI.py

The "Process Files" button creates 5 files. The files created are cap.csv, emit_r.csv, gen_ann.csv, gen_h and gen_ivrt. These files represent Capacity National (GW), CO2 Emissions BA (tonne), Generation National (TWh), Gen by timeslice regional/national(GW) and Generation ivrt (TWh) in Bokeh Pivot, respectively. These CSVs are placed in the runs directory, which mimics the directory structure created by ReEDS. 
    
Note: The cap.csv file is necessary for Bokeh Pivot to consider the directory to be from ReEDS.

The "Customize CSV" button is to be used when a graph is not automatically created and needs to be done from scratch. The mapping of each Dimension and value must be done according to the CSV file that will be mimicked.

The "Copy Runs Folder Path" button copies the runs directory path to the clipboard.

### Bokeh Pivot
Open the X2BokehPivot by selecting the correct environment and running the launch.bat. Once your browser opens, paste the runs directory path. If everything went correctly once a result and preset were chosen, then a graph should appear.
 
Note: To change the color of the technologies, navigate to X2BokehPivot/in/reeds2 and select the tech_style.csv to change the colors of each technology.




