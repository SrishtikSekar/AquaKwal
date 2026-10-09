"""Small local Spark smoke test for the ML input/output contract."""

import csv
from pathlib import Path
import shutil
import subprocess
import tempfile


PROJECT = Path(__file__).resolve().parents[1]


def main():
    if not shutil.which("spark-submit"):
        raise SystemExit("spark-submit is not installed")

    with tempfile.TemporaryDirectory() as tmp_name:
        tmp = Path(tmp_name)
        input_file = tmp / "clean.txt"
        output_dir = tmp / "ml-output"
        with input_file.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, delimiter="|", lineterminator="\n")
            for index in range(80):
                is_good = index % 2 == 0
                features = [
                    20 + index % 4, 24 + index % 4,
                    5.5 if is_good else 2.5, 6.5 if is_good else 3.5,
                    7.0 if is_good else 5.8, 8.0 if is_good else 9.1,
                    100 + index, 120 + index,
                    1.0 if is_good else 4.0, 2.0 if is_good else 5.0,
                    0.5, 1.0, 100, 200 if is_good else 3000,
                    300, 400, 10, 20,
                ]
                writer.writerow([
                    f"S{index}", f"Location {index}", 2022 + index % 2,
                    "RIVER", "STATE", *features, "GOOD" if is_good else "POOR",
                ])

        subprocess.run(
            [
                "spark-submit", "--master", "local[2]",
                str(PROJECT / "spark" / "ml_quality.py"),
                "--input", input_file.as_uri(), "--output", output_dir.as_uri(),
                "--cv-folds", "2", "--num-trees", "5", "--max-depths", "3",
            ],
            cwd=tmp, check=True,
        )

        required = ["predictions", "metrics", "feature_importances"]
        for name in required:
            directory = output_dir / name
            if not directory.is_dir() or not list(directory.glob("part-*")):
                raise AssertionError(f"missing Spark output: {name}")
        metric_text = "".join(
            path.read_text() for path in (output_dir / "metrics").glob("part-*.csv")
        )
        for metric in ("AUC", "F1", "Accuracy"):
            if metric not in metric_text:
                raise AssertionError(f"metric not written: {metric}")

        print("Spark integration test passed: input -> model -> three output datasets")


if __name__ == "__main__":
    main()
