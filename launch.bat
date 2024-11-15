@echo off
setlocal enabledelayedexpansion

:: Set the Miniforge installation path
set INSTALL_PATH=%LOCALAPPDATA%\Miniforge3

:: Set the name of the conda environment
set ENV_NAME=xml2csv

:: Set the paths to your Python scripts
set PYTHON_SCRIPT=Plexos2BokehPivot.py
set PYTHON_SCRIPT2=postrename.py
set PYTHON_SCRIPT3=postappend.py 
:: Activate Miniforge environment
echo Activating Miniforge environment '%ENV_NAME%'...
call "%INSTALL_PATH%\condabin\conda.bat" activate %ENV_NAME%

echo Running Python script '%PYTHON_SCRIPT%'...
python "%PYTHON_SCRIPT%"
python "%PYTHON_SCRIPT2%"
python "%PYTHON_SCRIPT3%"

:: Deactivate the conda environment
echo Deactivating the conda environment...
call conda deactivate

