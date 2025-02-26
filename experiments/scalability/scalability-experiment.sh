#!/bin/bash

# Default values
BUILD_DIR=""
RESULTS_DIR=""
DATA_DIR=""
EXPERIMENT_NAME=""
PYTHON_SCRIPT=""
NODE_COUNT=0

USAGE="Usage: scalability-experiment.sh -s PARENT_DIR/PYTHON_SCRIPT -b PATH_TO_BUILD_DIRECTORY -r PATH_TO_RESULTS_DIRECTORY -d PATH_TO_DATA_DIRECTORY -e NAME_OF_EXPERIMENT -n NODE_COUNT"

# Process arguments
while getopts "b:r:d:e:n:s:" opt; do
  case $opt in
    b) BUILD_DIR="$OPTARG" ;;
    r) RESULTS_DIR="$OPTARG" ;;
    d) DATA_DIR="$OPTARG" ;;
    e) EXPERIMENT_NAME="$OPTARG" ;;
    n) NODE_COUNT="$OPTARG" ;;
    s) PYTHON_SCRIPT="$OPTARG" ;;
    \?) echo "$USAGE"; exit 1 ;;
  esac
done

# Ensure all required arguments are provided
if [[ -z "$BUILD_DIR" || -z "$PYTHON_SCRIPT" || -z "$RESULTS_DIR" || -z "$DATA_DIR" || -z "$EXPERIMENT_NAME" ]]; then
    echo "Error: Missing required arguments."
    echo "$USAGE"
    exit 1
fi

for i in $(seq 1 3); do
    echo "Beginning iteration $i"
    python3 "$PYTHON_SCRIPT" --build-dir "$BUILD_DIR" --results-dir "$RESULTS_DIR" --data-dir "$DATA_DIR" --test-name "$EXPERIMENT_NAME" --node-count "$NODE_COUNT" || 
    {
        echo "Error in iteration $i"
        exit 1
    }
done
