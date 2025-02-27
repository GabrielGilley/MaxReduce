# Scalability Experiment
run build\_images.sh

apptainer, cmake and make

run "script.sh pipeline filters/pipeline.txt" to do a single iteration of the pipeline scalability test

run "script.sh shuffle filters/shuffle.txt" to do a single iteration of the shuffle scalability test

## Config
config.cfg exists for static paths. We'll have to put data directory in there. Presumably a directory of json files once we have that support, or a path to a single huge json file. Modify paths for your own system but I think they'll be the same if you use this repo.

## Issues
Completely untested. Snake will terminate immediately if the results files exist. After the first iteration, if you try to run again it will stop because the results files exist, so you'll have to move the files after each run or modify script.sh or some other solution.

There may be a pathing issue with the slurm script being in experiments instead of src (pando repo). NOTE that pardb.sbatch is modified in the experiment to allow for configurable nodes instead of just all of them, which is needed for scalability.

## par-script.py
Meant to be an abstract use-case for parallel pando work. It takes a lot of params at command line so using the snakefile is ideal as it can ingest a lot of the stuff from a config file. Future work could have the par-script take in config file too.

It takes filters as a text file where each is on its own line. this is just to make it a single argument and could be modified to whatever form factor is best. See filters/\* for example.

## Snakefile
Supposed to run the slurm script for each node count in the line 2 list.

Note pip install snakefile and pip install 'pulp<2.8' need to be run

## Weak scalability
Exact same code as strong scalability but we'll point it to smaller data directories. Whatever we decide (1 TB max maybe) for the 64 node cluster needs to be scaled down. Assuming 1024 divisions instead of 1000 (TiB vs TB), its node count \* 16 GiB. so The following:

8 nodes --> 128 GiB

16 nodes --> 256 GiB

32 nodes --> 512 GiB

64 nodes --> 1024 GiB
