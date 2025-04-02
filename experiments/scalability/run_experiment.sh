#!/bin/bash

if [[ $# -ne 5 ]]; then
  echo "Usage: $0 EXPERIMENT_NAME FILTER_LIST_IN_TXT_FILE DATA_DIR RESULTS_DIR NODE_COUNT"
  echo "Ex: $0 shuffle_scalability filters/shuffle.txt /cscratch/btc-export results 8"
  exit 1
fi

TEST_NAME=$1
FILTERS=$2
DATA_DIR=$3
RESULTS_DIR=$4
NODE_COUNT=$5

# Get the count of available idle processing nodes in the specified partition
AVAILABLE_NODE_COUNT=$(sinfo -N -p compute | grep idle | awk '{print $1}' | wc -l)

# Check if available nodes are less than requested nodes
if [ "$AVAILABLE_NODE_COUNT" -lt "$NODE_COUNT" ]; then
    echo "Error: Requested $NODE_COUNT nodes when only $AVAILABLE_NODE_COUNT nodes are available."
    exit 1
fi

# Clear space
echo "Removing BigSpace and Elga files across all available nodes. You have 5 seconds to cancel (CTRL+C)."
for i in {5..0}; do
    echo "$i"
    sleep 1
done

echo "Removing... Please wait..."
srun -N $AVAILABLE_NODE_COUNT bash -c 'sudo rm -rf /scratch/bigspace* /tmp/elga*'
echo "BigSpace and Elga files removed."


# Submit batch job
SLURM_OUTPUT=$(sbatch -N "$NODE_COUNT" pardb.sbatch)
JOB_ID=$(echo "$SLURM_OUTPUT" | awk '{print $4}')
echo "Batch job submitted with $NODE_COUNT nodes. Job ID: $JOB_ID"

sleep 1

# Get mesh
FIRST_NODE="en$(scontrol show job $JOB_ID | grep "NodeList=" | awk 'NR==2' | awk -F'=' '{print $2}' | tr -d ' ' | awk -F',' '{print $1}' | awk -F'[' '{print $2}' | awk -F'-' '{print $1}' | sed 's/[[:space:]]//g')"
HOST_OUTPUT=$(host "$FIRST_NODE.100g")
IP_ADDRESS=$(echo "$HOST_OUTPUT" | awk '{print $NF}')
MESH="${IP_ADDRESS},0"

echo "MESH: $MESH"

echo "Beginning Experiment..."
./../../src/sifs/pando_python.sif python3 par-script.py -n $TEST_NAME -m $MESH -f $FILTERS -d $DATA_DIR -r $RESULTS_DIR

echo "Experiment Complete"
echo "Canceling Job $JOB_ID"
scancel $JOB_ID

# HERE WE WANT TO RUN THE SNAKEFILE, MOVE THE OUTPUT FILES (OTHERWISE SNAKE WILL SEE THEY ALREADY EXIST AND TERMINATE AFTER FIRST RUN)
# RUN THE FILE, MOVE THE OUTPUT, RUN THE FILE. 
# snakemake --cores all --config experiment_name="$1" filters="$2" nodes="$3"

