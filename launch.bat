@echo off
:: Check if Mamba is installed
where mamba >nul 2>nul
if %errorlevel% neq 0 (
    echo Mamba is not installed. Please install Mamba first.
    exit /b 1
)

:: Define environment name and script paths
set ENV_NAME=xml2csv
set SCRIPT1=XML2CSV.py
set SCRIPT2=Plexos2BokehPivotGUI.py

:: Activate the mamba environment
echo Activating environment: %ENV_NAME%
call mamba activate %ENV_NAME%
if %errorlevel% neq 0 (
    echo Failed to activate the environment %ENV_NAME%.
    exit /b 1
)

:: Check if the first script exists and run it
if exist %SCRIPT1% (
    echo Running %SCRIPT1%...
    python %SCRIPT1%
    if %errorlevel% neq 0 (
        echo Failed to run %SCRIPT1%.
        exit /b 1
    )
) else (
    echo %SCRIPT1% not found.
    exit /b 1
)

:: Check if the second script exists and run it
if exist %SCRIPT2% (
    echo Running %SCRIPT2%...
    python %SCRIPT2%
    if %errorlevel% neq 0 (
        echo Failed to run %SCRIPT2%.
        exit /b 1
    )
) else (
    echo %SCRIPT2% not found.
    exit /b 1
)

:: Deactivate the environment
echo Deactivating environment...
call mamba deactivate
if %errorlevel% neq 0 (
    echo Failed to deactivate the environment.
    exit /b 1
)

echo All tasks completed successfully.
