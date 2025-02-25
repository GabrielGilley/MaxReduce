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
    next(reader)  # Skip the column titles
    for row in reader:
        # Date,Open,High,Low,Close,Adj Close,Volume
        [date_no_time, open_, high, low, close, adj_close, volume] = row

        symbol_no_usd = input_fn.split('-')[0]
        out_file.write("TAGS\n")
        out_file.write(f"{symbol_no_usd.upper()}\n")
        out_file.write("exchange_rate_raw\n")
        out_file.write(f"{date_no_time}\n")
        out_file.write("VALUE\n")
        out_file.write(f"{high}\n")
        out_file.write("END\n")

out_file.close()
