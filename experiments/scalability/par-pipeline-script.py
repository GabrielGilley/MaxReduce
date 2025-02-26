# Imports
from timeit import default_timer as timer
import sys
import os

# Globals
import argparse

# Argument Parser
parser = argparse.ArgumentParser(description="Process and time database operations.")
parser.add_argument("--build-dir", type=str, required=True, help="Path to the build directory")
parser.add_argument("--results-dir", type=str, required=True, help="Path to directory where to save results")
parser.add_argument("--data-dir", type=str, required=True, help="Path to the data directory")
parser.add_argument("--test-name", type=str, required=True, help="Name of the test run")
parser.add_argument("--ip", type=str, required=True, help="IP,ID of the ParDBClient")

# Parse arguments
args = parser.parse_args()
BUILD_DIR = args.build_dir
RESULTS_DIR = args.results_dir
DATA_DIR = args.data_dir
TEST_NAME = args.test_name
IP = args.ip

sys.path.append(os.path.join(BUILD_DIR, 'pybind11'))
import pando_api
os.makedirs(RESULTS_DIR, exist_ok=True)

# Create client
client = pando_api.ParDBClient(IP)

# Add filter directory
client.add_filter_dir(os.path.join(BUILD_DIR, 'filters'))

# We just need to get through tx vals to include cross-db lookup in scalability test
client.install_filter('BTC_block_to_tx');
client.install_filter('BTC_block_to_txtime');
client.install_filter('BTC_tx_vals');

# Begin test
start = timer()
client.process()  # Async
while client.processing():
    time.sleep(10) 
end = timer()

# Save results
with open(os.path.join(RESULTS_DIR, TEST_NAME + ".csv"), "a") as file:
    file.write(str(end - start) + ",")

