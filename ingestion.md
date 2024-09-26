# Ingesting New Data Into Pando

### Preparing data

The first step is to prepare the data that you want to ingest into Pando.
Copying the data from a *cleanly* stopped daemon into the `/cscratch` directory is the easiest approach.

Once the data is copied onto the machines, extract it.
Then, in the `json_rpc_parser` repo, you will need to run the `prepare_data.sh` script.
You should set the number of data directories to the max number of docker containers you will want to run for ingestion.
Example Usage:

```bash
./prepare_data.sh btc /cscratch/btc/ /cscratch/btc-data 200
```

NOTE: This can take a bit of time, depending on how many data directories you create and how big the blockchain is. 

### Dockerblockchains

Before ingesting data or starting the Pando DB, you will need to create the docker containers that will do the ingestion (NOTE: This needs to be done on every "compute" node in the cluster).
To do this, clone the dockerblockchains repo.
Then, run a command similar to the following (adjusting for paths as necessary):

```bash
srun -N $(sinfo | grep compute | awk '{print $4}') bash -c 'cd $HOME/pando_ldrd/dockerblockchains/bitcoin/ && make build'
```

### Ingest

To start the ingestion process, go to the `slurm_scripts` directory in the `json_rpc_parser` repo.

In the `parse.sbatch` file, there will be some parameters that may need changed:

- `CHAIN_ARGS` -- a space separated list of the name of the docker container, the path to the `get_block` script, and the RPC port of the blockchain.
- `CHAIN_ARGS_MSG` -- the string that appears in the logs of the blockchain that indicates that it is ready to be queried
- `MIN_BLOCK` -- the block to start ingesting at
- `MAX_BLOCK` -- the block to end ingesting at (typically will be the number of total blocks in the blockchain)
- `PANDO_MESH_ENTRY` -- the IP address and ID of any Pando node
- `PANDO_PROXY_BIN` -- the path to the `pando_proxy.sif` file
- `SCRATCH` -- the path to the location where the data directories are stored

Run the following command to start parsing docker containers on 5 machines, each of which run the default (currently 16) number of containers, for a total of 80 parsing containers:

```bash
sbatch -N 5 parse.sbatch
```

After a few minutes, you should see data being populated within the `pando_top` interface.
