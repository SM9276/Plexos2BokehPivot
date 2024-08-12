@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

:: Specify Miniforge version
set MINIFORGE_VERSION=latest

:: Set the installation path to the user's %LOCALAPPDATA% directory
set INSTALL_PATH=%LOCALAPPDATA%\Miniforge3

:: Download the Miniforge installer if not present
if not exist %INSTALL_PATH%\condabin\conda.bat (
    echo Miniforge not found. Downloading Miniforge installer...
    curl -O -L --insecure https://github.com/conda-forge/miniforge/releases/%MINIFORGE_VERSION%/download/Miniforge3-Windows-x86_64.exe
    echo Installing Miniforge...
    Miniforge3-Windows-x86_64.exe /S /D=%INSTALL_PATH%
    echo Cleaning up...
    del Miniforge3-Windows-x86_64.exe
)

:: Activate Miniforge
echo Activating Miniforge...
call %INSTALL_PATH%\condabin\conda.bat activate

:: Specify the environment name and the YAML file name
set ENV_NAME=xml2csv
set YAML_FILE=environment.yaml

:: Create conda environment from YAML file
echo Creating conda environment...
call mamba env create -n %ENV_NAME% -f %YAML_FILE%

:: Display environment creation success message
echo Conda environment '%ENV_NAME%' created successfully!

:: Construct the path to the Python executable directly
set "PYTHON_EXECUTABLE=%INSTALL_PATH%\envs\%ENV_NAME%\python.exe"

:: Print the path to the Python executable
echo Python executable path: %PYTHON_EXECUTABLE%

:: Write the cleaned path to python_executable_path.txt without adding whitespace
<nul set /p "=!PYTHON_EXECUTABLE!" > python_executable_path.txt

:: Display success message
echo Python executable path for %ENV_NAME% environment written to python_executable_path.txt

pause

:: ******************* from downloaded miniforge installer *******************

@REM @echo off

@REM :: Set the installation path to the user's %LOCALAPPDATA% directory
@REM set INSTALL_PATH=%LOCALAPPDATA%\Miniforge3

@REM :: Search for Miniforge installer in Downloads folder
@REM set "MINIFORGE_INSTALLER_PATH="
@REM for /r "%HOMEPATH%\Downloads" %%i in (Miniforge3-Windows-x86_64.exe) do (
@REM     set "MINIFORGE_INSTALLER_PATH=%%i"
@REM     goto :found
@REM )
@REM :found

@REM :: Check if the installer file exists
@REM if not exist "%MINIFORGE_INSTALLER_PATH%" (
@REM     echo Error: Miniforge installer not found in Downloads folder.
@REM     pause
@REM     exit /b 1
@REM )

@REM :: Install Miniforge
@REM echo Installing Miniforge...
@REM "%MINIFORGE_INSTALLER_PATH%" /S /D=%INSTALL_PATH%

@REM :: Display installation success message
@REM echo Miniforge installed successfully at %INSTALL_PATH%

@REM pause


:: ******************* automatic download and install (didn't work) *******************

@REM @echo off

@REM :: Specify Miniforge version
@REM set MINIFORGE_VERSION=latest

@REM :: Set the installation path to the user's %LOCALAPPDATA% directory
@REM set INSTALL_PATH=%LOCALAPPDATA%\Miniforge3
@REM echo %INSTALL_PATH%

@REM :: Download the Miniforge installer
@REM echo Downloading Miniforge installer...
@REM curl -O -L --insecure https://github.com/conda-forge/miniforge/releases/%MINIFORGE_VERSION%/download/Miniforge3-Windows-x86_64.exe

@REM echo Installing Miniforge...
@REM start /wait "" Miniforge3-Windows-x86_64.exe /InstallationType=JustMe /RegisterPython=0 /D=%INSTALL_PATH%

@REM :: Cleanup - remove the installer
@REM echo Cleaning up...

@REM :: echo current directory
@REM echo %cd%
@REM del Miniforge3-Windows-x86_64.exe

@REM :: Display installation success message
@REM echo Miniforge installed successfully at %INSTALL_PATH%

@REM pause
