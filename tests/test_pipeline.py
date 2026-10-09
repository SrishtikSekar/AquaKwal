import csv
import importlib.util
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


PROJECT = Path(__file__).resolve().parents[1]

HEADER = [
    "STN code", "monitoring_location", "year", "water_body_type", "state_name",
    "temp_min", "temp_max", "dissolved_min", "dissolved_max", "ph_min", "ph_max",
    "conductivity_min", "conductivity_max", "bod_min", "bod_max", "nitrate_min",
    "nitrate_max", "fecal_coliform_min", "fecal_coliform_max",
    "total_coliform_min", "total_coliform_max", "fecal_min", "fecal_max",
]


def load_converter_module():
    spec = importlib.util.spec_from_file_location(
        "convert_csv", PROJECT / "data" / "convert_csv.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_pig_fixture(path):
    good = [
        "A1", "River, North", "2022", "RIVER", "STATE A", "20", "21", "5", "6",
        "7", "8", "100", "110", "1", "2", "0.5", "1", "100", "200", "300",
        "400", "10", "20",
    ]
    low_oxygen = [
        "A2", "Lake", "2022", "LAKE", "STATE A", "30", "31", "3", "5", "7", "8",
        "120", "130", "1", "2", "0.5", "1", "100", "200", "300", "400", "10", "20",
    ]
    missing_and_poor = [
        "A3", "Reservoir", "2023", "LAKE", "STATE B", "", "25", "5", "6", "6", "9",
        "140", "150", "2", "4", "1", "2", "100", "3000", "500", "600", "20", "30",
    ]
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, delimiter="|", lineterminator="\n")
        writer.writerows([HEADER, good, good, low_oxygen, missing_and_poor])


def read_part_rows(directory, delimiter="|"):
    rows = []
    for part in sorted(directory.glob("part*")):
        with part.open(encoding="utf-8", newline="") as stream:
            rows.extend(csv.reader(stream, delimiter=delimiter))
    return rows


class ConverterTests(unittest.TestCase):
    def test_converter_preserves_embedded_commas_and_validates_width(self):
        converter = load_converter_module()
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            source = tmp / "input.csv"
            output = tmp / "output.txt"
            row = ["A1", "River, North"] + [str(i) for i in range(21)]
            with source.open("w", encoding="utf-8", newline="") as stream:
                csv.writer(stream).writerows([HEADER, row])

            self.assertEqual(converter.convert_csv(source, output), 2)
            with output.open(encoding="utf-8", newline="") as stream:
                converted = list(csv.reader(stream, delimiter="|"))
            self.assertEqual(converted[1][1], "River, North")
            self.assertEqual(len(converted[1]), 23)

            source.write_text("only,two\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "expected 23"):
                converter.convert_csv(source, output)

            bad_row = row.copy()
            bad_row[1] = "unsafe|location"
            with source.open("w", encoding="utf-8", newline="") as stream:
                csv.writer(stream).writerow(bad_row)
            converter.convert_csv(source, output)
            with output.open(encoding="utf-8", newline="") as stream:
                sanitized = list(csv.reader(stream, delimiter="|"))
            self.assertEqual(sanitized[0][1], "unsafe/location")

    def test_converter_normalizes_public_22_column_river_source(self):
        converter = load_converter_module()
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            source = tmp / "river.csv"
            output = tmp / "output.txt"
            public_header = ["Station Code", "Monitoring Location", "State"] + [
                f"feature_{index}" for index in range(18)
            ] + ["Year"]
            public_row = ["1001", "RIVER BEAS", "HIMACHAL PRADESH"] + [
                str(index) for index in range(18)
            ] + ["2023"]
            with source.open("w", encoding="utf-8", newline="") as stream:
                csv.writer(stream).writerows([public_header, public_row])

            self.assertEqual(converter.convert_csv(source, output), 2)
            with output.open(encoding="utf-8", newline="") as stream:
                converted = list(csv.reader(stream, delimiter="|"))
            self.assertEqual(converted[0], converter.OUTPUT_HEADER)
            self.assertEqual(converted[1][:5], [
                "1001", "RIVER BEAS", "2023", "RIVER", "HIMACHAL PRADESH"
            ])
            self.assertEqual(len(converted[1]), 23)


@unittest.skipUnless(shutil.which("pig"), "Apache Pig is not installed")
class PigFlowTests(unittest.TestCase):
    def test_clean_and_enriched_input_output_flow(self):
        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            raw = tmp / "raw.txt"
            clean = tmp / "clean"
            enriched = tmp / "enriched"
            thresholds = tmp / "thresholds.csv"
            write_pig_fixture(raw)
            thresholds.write_text(
                "dissolved_min,ph_min,ph_max,bod_max,fecal_coliform_max\n"
                "4.0,6.5,8.5,3.0,2500\n",
                encoding="utf-8",
            )

            subprocess.run(
                [
                    "pig", "-x", "local", "-stop_on_failure",
                    "-param", f"INPUT={raw}",
                    "-param", f"CLEAN_OUTPUT={clean}",
                    "-f", str(PROJECT / "pig" / "etl_clean.pig"),
                ],
                cwd=tmp, check=True, capture_output=True, text=True,
            )
            clean_rows = read_part_rows(clean)
            self.assertEqual(len(clean_rows), 3, "exact duplicate should be removed")
            self.assertTrue(all(len(row) == 24 for row in clean_rows))
            by_station = {row[0]: row for row in clean_rows}
            self.assertEqual(by_station["A1"][1], "River, North")
            self.assertEqual(by_station["A1"][-1], "GOOD")
            self.assertEqual(by_station["A2"][-1], "POOR")
            self.assertEqual(by_station["A3"][-1], "POOR")
            self.assertAlmostEqual(float(by_station["A3"][5]), 23.3333, places=3)

            subprocess.run(
                [
                    "pig", "-x", "local", "-stop_on_failure",
                    "-param", f"CLEAN_INPUT={clean}",
                    "-param", f"THRESHOLDS_INPUT={thresholds}",
                    "-param", f"ENRICHED_OUTPUT={enriched}",
                    "-f", str(PROJECT / "pig" / "sample_source_join.pig"),
                ],
                cwd=tmp, check=True, capture_output=True, text=True,
            )
            enriched_rows = read_part_rows(enriched)
            self.assertEqual(len(enriched_rows), 3)
            self.assertTrue(all(len(row) == 25 for row in enriched_rows))
            violations = {row[0]: int(row[-1]) for row in enriched_rows}
            self.assertEqual(violations, {"A1": 0, "A2": 1, "A3": 3})


class ContractTests(unittest.TestCase):
    def test_hive_and_spark_use_the_pig_output_delimiter(self):
        hive = (PROJECT / "hive" / "warehouse.hql").read_text()
        spark = (PROJECT / "spark" / "ml_quality.py").read_text()
        self.assertEqual(hive.count("FIELDS TERMINATED BY '|'"), 2)
        self.assertIn('sep="|"', spark)

    def test_scripts_and_python_parse(self):
        subprocess.run(
            ["bash", "-n", "scripts/run_pipeline.sh", "scripts/demo.sh", "docker/bootstrap.sh"],
            cwd=PROJECT, check=True,
        )
        subprocess.run(
            ["python3", "-m", "py_compile", "data/convert_csv.py", "spark/ml_quality.py"],
            cwd=PROJECT, check=True,
        )

    @unittest.skipUnless(shutil.which("docker"), "Docker is not installed")
    def test_compose_file_is_valid(self):
        subprocess.run(
            ["docker", "compose", "-f", "docker/docker-compose.yml", "config", "--quiet"],
            cwd=PROJECT, check=True,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
