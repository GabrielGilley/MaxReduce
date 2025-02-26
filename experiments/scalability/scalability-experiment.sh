#!/bin/bash

# Default values
BUILD_DIR=""
RESULTS_DIR=""
DATA_DIR=""
EXPERIMENT_NAME=""
PYTHON_SCRIPT=""
CLIENT_ADDRESS=""
NODE_COUNT=0

USAGE="Usage: scalability-experiment.sh -s PARENT_DIR/PYTHON_SCRIPT -b PATH_TO_BUILD_DIRECTORY -r PATH_TO_RESULTS_DIRECTORY -d PATH_TO_DATA_DIRECTORY -e NAME_OF_EXPERIMENT -n NODE_COUNT -ip CLIENT_ADDRESS"

# Process arguments
while getopts "b:r:d:e:n:s:ip:" opt; do
  case $opt in
    b) BUILD_DIR="$OPTARG" ;;
    r) RESULTS_DIR="$OPTARG" ;;
    d) DATA_DIR="$OPTARG" ;;
    e) EXPERIMENT_NAME="$OPTARG" ;;
    n) NODE_COUNT="$OPTARG" ;;
    s) PYTHON_SCRIPT="$OPTARG" ;;
    ip) CLIENT_ADDRESS="$OPTARG" ;;
    \?) echo "$USAGE"; exit 1 ;;
  esac
done

# Ensure all required arguments are provided
if [[ -z "$BUILD_DIR" || -z "$CLIENT_ADDRESS" || -z "$PYTHON_SCRIPT" || -z "$RESULTS_DIR" || -z "$DATA_DIR" || -z "$EXPERIMENT_NAME" ]]; then
    echo "Error: Missing required arguments."
    echo "$USAGE"
    exit 1
fi

for i in $(seq 1 3); do
    echo "Beginning iteration $i"
    python3 "$PYTHON_SCRIPT" --build-dir "$BUILD_DIR" --ip "$CLIENT_ADDRESS" --results-dir "$RESULTS_DIR" --data-dir "$DATA_DIR" --test-name "{$EXPERIMENT_NAME}_{$NODE_COUNT}" || 
    {
        echo "Error in iteration $i"
        echo "Bash script not meant to use in seq python script, use parallel"
        exit 1 
    }
done
