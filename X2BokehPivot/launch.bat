@echo off
setlocal enabledelayedexpansion

:: Set the installation path to Miniforge
set INSTALL_PATH=%LOCALAPPDATA%\Miniforge3

:: Specify the environment name
set ENV_NAME=bokehpivot

:: Check if Miniforge is installed
if not exist "%INSTALL_PATH%\condabin\conda.bat" (
    echo Miniforge not found. Please install Miniforge before running this script.
    exit /b 1
)

:: Activate Miniforge and the environment
echo Activating Miniforge and the '%ENV_NAME%' environment...
call "%INSTALL_PATH%\condabin\conda.bat" activate %ENV_NAME%

:: Check if environment activation was successful
if errorlevel 1 (
    echo Failed to activate the environment. Exiting...
    exit /b 1
)

call bokeh serve . --sh --port 0
cmd /k
