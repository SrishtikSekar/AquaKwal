#!/usr/bin/env python3
"""
Convert CSV with quoted fields to pipe-delimited format for Pig.
"""
import csv
import sys

input_file = '/home/gsrishtik/AquaKwal/data/Indian_water_data.csv'
output_file = '/home/gsrishtik/AquaKwal/data/Indian_water_data_pipe.csv'

with open(input_file, 'r', encoding='utf-8') as infile, \
     open(output_file, 'w', encoding='utf-8') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile, delimiter='|', quoting=csv.QUOTE_MINIMAL)
    for row in reader:
        writer.writerow(row)

print(f"Converted {input_file} -> {output_file}")
print("Done")