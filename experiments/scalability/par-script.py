# Imports
from timeit import default_timer as timer
import sys
import os
import argparse

# Argument Parser
parser = argparse.ArgumentParser(description="")
parser.add_argument("--build_dir", type=str, required=True, help="Path to the build directory")
parser.add_argument("--results_dir", type=str, required=True, help="Path to directory where to save results")
parser.add_argument("--data_dir", type=str, required=True, help="Path to the data directory")
parser.add_argument("--test_name", type=str, required=True, help="Name of the test run")
parser.add_argument("--ip", type=str, required=True, help="IP,ID of the ParDBClient")
parser.add_argument("--filters", type=str, required=True, help="Text file with a list of filters on eachtheir own lines")

# Parse arguments
args = parser.parse_args()
BUILD_DIR = args.build_dir
RESULTS_DIR = args.results_dir
DATA_DIR = args.data_dir
TEST_NAME = args.test_name
IP = args.ip
FILTER_FILE = args.filters

sys.path.append(os.path.join(BUILD_DIR, 'pybind11'))
import pando_api
os.makedirs(RESULTS_DIR, exist_ok=True)

# Create client
client = pando_api.ParDBClient(IP)

# Add filter directory
client.add_filter_dir(os.path.join(BUILD_DIR, 'filters'))

# Import data. This will need to be modified depending on how JSON gets implemented
client.import_db(DATA_DIR)

# Install all filters
with open(FILTER_FILE, "r") as readable:
    for line in readable:
        client.install_filter(line.strip())  # Strip newline and carriage return

# Begin test
start = timer()
client.process()  # Async
while client.processing():
    time.sleep(10) 
end = timer()

# Save results
with open(os.path.join(RESULTS_DIR, TEST_NAME + ".csv"), "a") as file:
    file.write(str(end - start) + ",")

