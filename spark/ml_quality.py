#!/usr/bin/env python3
"""
AquaKwal: Indian Water Quality Analytics Pipeline
File: spark/ml_quality.py
Purpose: Read Pig-cleaned Indian water quality data from HDFS and train
         a Spark MLlib classifier to predict water quality (GOOD/POOR).
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

HDFS_INPUT_PATH = "/data/clean/water_quality_clean"
HDFS_OUTPUT_DIR = "/data/output/ml_results"

FEATURE_COLS = [
    "temp_min", "temp_max", "dissolved_min", "dissolved_max",
    "ph_min", "ph_max", "conductivity_min", "conductivity_max",
    "bod_min", "bod_max", "nitrate_min", "nitrate_max",
    "fecal_coliform_min", "fecal_coliform_max",
    "total_coliform_min", "total_coliform_max",
    "fecal_min", "fecal_max",
]


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=HDFS_INPUT_PATH)
    parser.add_argument("--output", default=HDFS_OUTPUT_DIR)
    parser.add_argument("--master", help="Spark master (for example local[2])")
    parser.add_argument("--cv-folds", type=int, default=3)
    parser.add_argument(
        "--num-trees", type=int, nargs="+", default=[50, 100],
        help="RandomForest tree counts used by cross-validation",
    )
    parser.add_argument(
        "--max-depths", type=int, nargs="+", default=[5, 8],
        help="RandomForest depths used by cross-validation",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.cv_folds < 2:
        raise ValueError("--cv-folds must be at least 2")

    builder = SparkSession.builder.appName(
        "AquaKwal-IndianWaterQuality-Classifier"
    )
    if args.master:
        builder = builder.master(args.master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("[1/6] Loading cleaned data:", args.input)

    schema = StructType([
        StructField("stn_code", StringType(), True),
        StructField("monitoring_location", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("water_body_type", StringType(), True),
        StructField("state_name", StringType(), True),
        StructField("temp_min", FloatType(), True),
        StructField("temp_max", FloatType(), True),
        StructField("dissolved_min", FloatType(), True),
        StructField("dissolved_max", FloatType(), True),
        StructField("ph_min", FloatType(), True),
        StructField("ph_max", FloatType(), True),
        StructField("conductivity_min", FloatType(), True),
        StructField("conductivity_max", FloatType(), True),
        StructField("bod_min", FloatType(), True),
        StructField("bod_max", FloatType(), True),
        StructField("nitrate_min", FloatType(), True),
        StructField("nitrate_max", FloatType(), True),
        StructField("fecal_coliform_min", FloatType(), True),
        StructField("fecal_coliform_max", FloatType(), True),
        StructField("total_coliform_min", FloatType(), True),
        StructField("total_coliform_max", FloatType(), True),
        StructField("fecal_min", FloatType(), True),
        StructField("fecal_max", FloatType(), True),
        StructField("water_quality_label", StringType(), True),
    ])

    df = spark.read.csv(args.input, header=False, sep="|", schema=schema)
    df = df.na.drop(subset=FEATURE_COLS + ["water_quality_label"])

    row_count = df.count()
    label_counts = df.groupBy("water_quality_label").count().collect()
    print("  Rows loaded:", row_count)
    for row in sorted(label_counts, key=lambda item: item["water_quality_label"]):
        print("  Label {}: {}".format(row["water_quality_label"], row["count"]))
    if row_count == 0:
        raise ValueError("no usable input rows remained after null filtering")
    if len(label_counts) != 2:
        raise ValueError("classification requires exactly two label classes")

    label_indexer = StringIndexer(
        inputCol="water_quality_label", outputCol="label", handleInvalid="skip"
    )

    assembler = VectorAssembler(
        inputCols=FEATURE_COLS, outputCol="features"
    )

    rf = RandomForestClassifier(
        labelCol="label", featuresCol="features", predictionCol="prediction",
        numTrees=100, maxDepth=8, seed=42,
    )

    pipeline = Pipeline(stages=[label_indexer, assembler, rf])

    train, test = df.randomSplit([0.8, 0.2], seed=42)
    train_count = train.count()
    test_count = test.count()
    print("[2/6] Train rows:", train_count, "  Test rows:", test_count)
    if train_count == 0 or test_count == 0:
        raise ValueError("the 80/20 split produced an empty train or test set")
    if train.select("water_quality_label").distinct().count() != 2:
        raise ValueError("training split does not contain both label classes")
    if test.select("water_quality_label").distinct().count() != 2:
        raise ValueError("test split does not contain both label classes")

    param_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, args.num_trees)
        .addGrid(rf.maxDepth, args.max_depths)
        .build()
    )

    evaluator_auc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator_auc,
        numFolds=args.cv_folds,
        seed=42,
    )

    grid_size = len(args.num_trees) * len(args.max_depths)
    print("[3/6] Training RandomForest ({}-fold CV, {}-param grid)...".format(
        args.cv_folds, grid_size
    ))
    cv_model = cv.fit(train)

    print("[4/6] Evaluating on test set...")
    predictions = cv_model.transform(test)

    auc = evaluator_auc.evaluate(predictions)
    print("  AUC (areaUnderROC) on test set: {:.4f}".format(auc))

    mc_eval = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1"
    )
    f1 = mc_eval.evaluate(predictions)
    print("  F1 score on test set: {:.4f}".format(f1))

    acc_eval = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    )
    accuracy = acc_eval.evaluate(predictions)
    print("  Accuracy on test set: {:.4f}".format(accuracy))

    print("  Confusion Matrix:")
    predictions.groupBy("label", "prediction").count().orderBy(
        "label", "prediction"
    ).show()

    best_model = cv_model.bestModel
    rf_model = best_model.stages[-1]

    importances = rf_model.featureImportances.toArray()
    feat_imp = list(zip(FEATURE_COLS, importances))
    feat_imp.sort(key=lambda x: x[1], reverse=True)
    print("  Feature Importances (top features):")
    for name, imp in feat_imp:
        print("    {:<26s} {:.4f}".format(name, imp))

    print("[5/6] Saving results:", args.output)

    predictions.select(
        "stn_code", "monitoring_location", "year", "water_body_type", "state_name",
        *FEATURE_COLS, "water_quality_label", "prediction", "probability",
    ).write.mode("overwrite").parquet(args.output + "/predictions")

    metrics = [
        ("AUC", round(auc, 4)),
        ("F1", round(f1, 4)),
        ("Accuracy", round(accuracy, 4)),
    ]
    metrics_df = spark.createDataFrame(metrics, ["metric", "value"])
    metrics_df.coalesce(1).write.mode("overwrite").csv(
        args.output + "/metrics", header=True
    )

    fi_rdd = spark.sparkContext.parallelize(
        [(name, float(imp)) for name, imp in feat_imp]
    )
    fi_df = spark.createDataFrame(fi_rdd, ["feature", "importance"])
    fi_df.coalesce(1).write.mode("overwrite").csv(
        args.output + "/feature_importances", header=True
    )

    print("[6/6] Done. Pipeline complete: Pig ETL -> Hive DDL -> Spark MLlib")
    spark.stop()


if __name__ == "__main__":
    main()
