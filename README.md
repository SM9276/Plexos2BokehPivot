# PLEXOS2BokehPivot

## 1) Getting started
Two python enviorments are used in this project, one to interact with the PLEXOS API and another to interact with ReEDS Bokeh Pivot. 

The enviorment.yml file found in the root directory is the to be used with XML2CSV.py and Plexos2BokehPivotGUI.py. Also make sure to have the PLEXOS API installed.

 Another enviorment.yml will be found in the X2BokehPivot directory this is to be used with BokehPivot.

 ## 2) Starting the program

### PLEXOS Solution to CSV 
Once the Plexos Solution files is gathered, place them into the PlexosSolutions directory. Take a look at config.csv, make sure to have all the correct parameters. Select the correct enviorment and run XML2CSV.py. In the PlexosOutputs directory each Scenerio will have its own dedicated directory with the collections that was processed. 

Note: In the current state of the script, currently, the script only processes LTPlans since it was hard coded but the following lines can change it in line 174 and 198 to switch it to STSchedule.

    SimulationPhaseEnum.LTPlan
Must be changed to 

    SimulationPhaseEnum.STSchedule

### PLEXOS CSV to ReEDS CSV
Next run the Plexos2BokehPivotGUI.py

The "Process Files" is creates 5 files. The files created are cap.csv, emit_r.csv, gen_ann.csv, gen_h and gen_ivrt. These files represent Capacity National (GW), CO2 Emissions BA (tonne), Generation National (TWh), Gen by timeslice regional/national(GW) and Generation ivrt (TWh) in bokeh pivot respectivly. These CSV are placed into the runs directory that mimics the directory structure created by ReEDS. 
    
Note: The cap.csv file is nesseccery for Bokeh Pivot to consider the directory to be from ReEDS 

The "Customize CSV" button is to be used when a graph is not automatically created, and needs to be done from scratch. The mapping of each Dimention and value must be done according to the csv file that will be mimiced.

The "Copy Runs Folder Path" button copies the runs directory path to the clipboard.

### Bokeh Pivot
Open the X2BokehPivot by selecting the correct enviorment and running the launch.bat, once your browser opens paste the runs directory path. If everything went correctly once a result and preset is choosen then a graph should appear.
 
Note: To change the color of the technologies, navigate to X2BokehPivot/in/reeds2 and select the tech_style.csv to change the colors of each technology.




