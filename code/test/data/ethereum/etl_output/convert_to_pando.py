#!/usr/bin/env python3

import csv
import sys

try:
    input_fn = sys.argv[1]
    output_fn = sys.argv[2]
except IndexError:
    print("Usage: ./convert_to_pando.py <csv input> <pando output fn>")
    sys.exit(1)

out_file = open(output_fn, 'w')
with open(input_fn) as f:
    reader = csv.reader(f)
    next(reader) #Skip the column titles
    if input_fn.startswith("tokens_"):
        for row in reader:
            [address, symbol, name, decimals, total_supply, block_number] = row
            out_file.write("TAGS\n")
            if address: out_file.write(f"{address}\n")
            if symbol: out_file.write(f"{symbol}\n")
            if name: out_file.write(f"{name}\n")
            out_file.write("ethereum_etl_token_address\n")
            out_file.write("VALUE\n")
            out_file.write(f"address:{address}\n")
            out_file.write(f"symbol:{symbol}\n")
            out_file.write(f"name:{name}\n")
            out_file.write(f"supply:{total_supply}\n")
            out_file.write("END\n")
    # TODO
    if input_fn.startswith("token_transfers_"):
        for row in reader:
            [token_address, from_address, to_address, value, transaction_hash, log_index, block_number] = row
            out_file.write("TAGS\n")
            if token_address: out_file.write(f"{token_address}\n")
            out_file.write("ethereum_etl_token_transfer\n")
            out_file.write("VALUE\n")
            out_file.write(f"token_address:{token_address}\n")
            out_file.write(f"from_address:{from_address}\n")
            out_file.write(f"to_address:{to_address}\n")
            out_file.write(f"tx_hash:{transaction_hash}\n")
            out_file.write(f"value:{value}\n")
            out_file.write(f"block:{block_number}\n")
            out_file.write("END\n")

out_file.close()
