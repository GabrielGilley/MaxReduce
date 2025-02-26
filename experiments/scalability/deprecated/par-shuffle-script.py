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
parser.add_argument("--ip", type=str, required=True, help="IP,ID of the ParDB client")

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

# Create db
client = pando_api.ParDBClient(IP)

# Add filter directory
client.add_filter_dir(os.path.join(BUILD_DIR, 'filters'))

# Import data. This will need to be modified depending on how JSON gets implemented
client.import_db(DATA_DIR)

# We only need to force arbitrary joins in the tx extraction
client.install_filter('python_BTC_block_to_tx_arbitrary_joins');

# Begin test
start = timer()
client.process()
end = timer()

# Save results
with open(os.path.join(RESULTS_DIR, TEST_NAME + ".csv"), "a") as file:
    file.write(str(end - start) + ",")

