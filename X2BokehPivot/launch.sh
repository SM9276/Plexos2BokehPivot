#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

# Set the installation path to Miniforge
INSTALL_PATH="$HOME/Miniforge3"

# Specify the environment name
ENV_NAME="bokehpivot"

# Check if Miniforge is installed
if [ ! -f "$INSTALL_PATH/bin/conda" ]; then
    echo "Miniforge not found. Please install Miniforge before running this script."
    exit 1
fi

# Activate Miniforge
echo "Activating Miniforge..."
source "$INSTALL_PATH/bin/activate"

# Check if activation was successful
if [ $? -ne 0 ]; then
    echo "Failed to activate Miniforge. Exiting..."
    exit 1
fi

# Activate the environment
echo "Activating the '$ENV_NAME' environment..."
conda activate "$ENV_NAME"

# Check if environment activation was successful
if [ $? -ne 0 ]; then
    echo "Failed to activate the environment '$ENV_NAME'. Exiting..."
    exit 1
fi

# Run bokeh server
echo "Starting bokeh server..."
bokeh serve . --sh --port 0

# Keep the terminal open (if needed)
exec bash

