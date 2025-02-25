#!/usr/bin/env python3

import csv
import sys
from pathlib import Path

try:
    input_fn = sys.argv[1]
    output_fn = sys.argv[2]
except IndexError:
    print("Usage: ./convert_token_exhange_to_pando.py <CSV input directory> <pando output fn>")
    sys.exit(1)

path = Path(input_fn)
date_a = {}

for infile in path.iterdir():

    if not infile.suffix == ".csv":
        continue

    with infile.open("r") as f:
        reader = csv.reader(f)
        next(reader) # Skip the acknowledgement of the data source
        next(reader)  # Skip the column titles
        for row in reader:
            if "Bitfinex" in infile.name:
                [unix, date, symbol, open_, high, low, close, volume_usd, volume_token] = row
                date_no_time = date.split(' ')[0]
            elif "Bittrex" in infile.name:
                [unix, date, symbol, open_, high, low, close, volume_usd, volume_token] = row
                date_no_time = date.split('T')[0]
            elif "CEX" in infile.name:
                [unix, date, symbol, open_, high, low, close, volume_token, volume_usd] = row
                date_no_time = date
            else:
                continue

            symbol_no_usd = symbol.split('/')[0]
            date_a[date_no_time] = date_a.get(date_no_time, [])
            date_a[date_no_time].append((symbol_no_usd.upper(), high))


with open(output_fn, "w") as out_file:
    for date, data in date_a.items():
        out_file.write("TAGS\n")
        out_file.write("ETL_TOKEN\n")
        out_file.write("exchange_rate_raw\n")
        out_file.write(f"{date}\n")
        out_file.write("VALUE\n")
        for token, value in data:
            out_file.write(f"{token}:{value}\n")
        out_file.write("END\n")
