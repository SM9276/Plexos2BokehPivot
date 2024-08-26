#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

# Change to the directory where the script is located
cd "$(dirname "$0")"

# Specify Miniforge version
MINIFORGE_VERSION="latest"

# Set the installation path to the user's home directory
INSTALL_PATH="$HOME/Miniforge3"

# Download the Miniforge installer if not present
if [ ! -f "$INSTALL_PATH/bin/conda" ]; then
    echo "Miniforge not found. Downloading Miniforge installer..."
    
    # Determine the URL for the Miniforge installer
    MINIFORGE_URL="https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh"
    
    # Download the installer
    curl -L -O "$MINIFORGE_URL"
    
    # Run the installer
    chmod +x Miniforge3-Linux-x86_64.sh
    ./Miniforge3-Linux-x86_64.sh -b -p "$INSTALL_PATH"
    
    # Clean up the installer
    rm Miniforge3-Linux-x86_64.sh
fi

# Activate Miniforge
echo "Activating Miniforge..."
source "$INSTALL_PATH/bin/activate"

# Specify the environment name and the YAML file name
ENV_NAME="bokehpivot"
YAML_FILE="environment.yaml"

# Create conda environment from YAML file
echo "Creating conda environment..."
mamba env create -n "$ENV_NAME" -f "$YAML_FILE"

# Display environment creation success message
echo "Conda environment '$ENV_NAME' created successfully!"

# Construct the path to the Python executable directly
PYTHON_EXECUTABLE="$INSTALL_PATH/envs/$ENV_NAME/bin/python"

# Print the path to the Python executable
echo "Python executable path: $PYTHON_EXECUTABLE"

# Write the cleaned path to python_executable_path.txt without adding whitespace
echo -n "$PYTHON_EXECUTABLE" > python_executable_path.txt

# Display success message
echo "Python executable path for $ENV_NAME environment written to python_executable_path.txt"

