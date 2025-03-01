#!/bin/bash

if [[ $# -ne 3 ]]; then
  echo "Usage: $0 experiment_name filter_list.txt NODE_COUNT"
  echo "Ex: $0 shuffle_scalability filters/shuffle.txt 8"
  exit 1
fi

# HERE WE WANT TO RUN THE SNAKEFILE, MOVE THE OUTPUT FILES (OTHERWISE SNAKE WILL SEE THEY ALREADY EXIST AND TERMINATE AFTER FIRST RUN)
# RUN THE FILE, MOVE THE OUTPUT, RUN THE FILE. 
snakemake --cores all --config experiment_name="$1" filters="$2" nodes="$3"

