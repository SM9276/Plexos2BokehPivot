@echo off
setlocal enabledelayedexpansion

:: Set the Miniforge installation path
set INSTALL_PATH=%LOCALAPPDATA%\Miniforge3

:: Set the name of the conda environment
set ENV_NAME=xml2csv

:: Set the paths to your Python scripts
set PYTHON_SCRIPT1=XML2CSV.py
set PYTHON_SCRIPT2=Plexos2BokehPivotGUI.py

:: Activate Miniforge environment
echo Activating Miniforge environment '%ENV_NAME%'...
call "%INSTALL_PATH%\condabin\conda.bat" activate %ENV_NAME%

:: Run the first Python script
echo Running Python script '%PYTHON_SCRIPT1%'...
python "%PYTHON_SCRIPT1%"

:: Run the second Python script
echo Running Python script '%PYTHON_SCRIPT2%'...
python "%PYTHON_SCRIPT2%"

:: Deactivate the conda environment
echo Deactivating the conda environment...
call conda deactivate

pause