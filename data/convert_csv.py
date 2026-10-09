#!/usr/bin/env python3
"""Convert a quoted CPCB CSV file to the pipe-delimited input used by Pig."""

import argparse
import csv
from pathlib import Path

EXPECTED_COLUMNS = 23
PUBLIC_RIVER_COLUMNS = 22

OUTPUT_HEADER = [
    "STN code", "monitoring_location", "year", "water_body_type", "state_name",
    "temp_min", "temp_max", "dissolved_min", "dissolved_max", "ph_min", "ph_max",
    "conductivity_min", "conductivity_max", "bod_min", "bod_max", "nitrate_min",
    "nitrate_max", "fecal_coliform_min", "fecal_coliform_max",
    "total_coliform_min", "total_coliform_max", "fecal_min", "fecal_max",
]


def convert_csv(input_file: Path, output_file: Path) -> int:
    """Convert *input_file* and return the number of rows written."""
    row_count = 0
    with input_file.open("r", encoding="utf-8-sig", newline="") as infile, \
            output_file.open("w", encoding="utf-8", newline="") as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(
            outfile, delimiter="|", quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )
        for line_number, row in enumerate(reader, start=1):
            if len(row) == PUBLIC_RIVER_COLUMNS:
                if line_number == 1:
                    row = OUTPUT_HEADER
                else:
                    # Public CPCB-derived river data stores State third and
                    # Year last, and all records are river samples.
                    row = [row[0], row[1], row[-1], "RIVER", row[2], *row[3:-1]]
            elif len(row) != EXPECTED_COLUMNS:
                raise ValueError(
                    f"row {line_number} has {len(row)} columns; "
                    f"expected {EXPECTED_COLUMNS} (native) or "
                    f"{PUBLIC_RIVER_COLUMNS} (public river source)"
                )
            # PigStorage has no CSV-style quoting. Collapse embedded newlines
            # and replace literal delimiters so every logical CSV row remains
            # exactly one physical, 23-field Pig record.
            row = [" ".join(field.split()).replace("|", "/") for field in row]
            writer.writerow(row)
            row_count += 1
    return row_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="source CPCB CSV")
    parser.add_argument("output", type=Path, help="destination pipe-delimited file")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = convert_csv(args.input, args.output)
    print(f"Converted {rows} rows: {args.input} -> {args.output}")


if __name__ == "__main__":
    main()
