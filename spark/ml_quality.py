#!/usr/bin/env python3
"""
AquaKwal: Indian Water Quality Analytics Pipeline
File: spark/ml_quality.py
Purpose: Read Pig-cleaned Indian water quality data from HDFS and train
         a Spark MLlib classifier to predict water quality (GOOD/POOR).
"""

import sys

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


def main():
    spark = (
        SparkSession.builder
        .appName("AquaKwal-IndianWaterQuality-Classifier")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("[1/6] Loading cleaned data from HDFS:", HDFS_INPUT_PATH)

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

    df = spark.read.csv(HDFS_INPUT_PATH, header=False, schema=schema)
    df = df.na.drop(subset=FEATURE_COLS + ["water_quality_label"])

    print("  Rows loaded:", df.count())
    df.groupBy("water_quality_label").count().show()

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

    pipeline = Pipeline(stages=[StringIndexer(inputCol="water_quality_label", outputCol="label", handleInvalid="skip"), 
                                VectorAssembler(inputCols=FEATURE_COLS, outputCol="features"), 
                                RandomForestClassifier(labelCol="label", featuresCol="features", predictionCol="prediction", numTrees=100, maxDepth=8, seed=42)])

    train, test = df.randomSplit([0.8, 0.2], seed=42)
    print("[2/6] Train rows:", train.count(), "  Test rows:", test.count())

    param_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [50, 100])
        .addGrid(rf.maxDepth, [5, 8])
        .build()
    )

    evaluator_auc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator_auc,
        numFolds=3,
        seed=42,
    )

    print("[3/6] Training RandomForest (3-fold CV, 4-param grid)...")
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
    predictions.select("label", "prediction").distinct().orderBy("label", "prediction").show()

    best_model = cv_model.bestModel
    rf_model = best_model.stages[-1]

    importances = rf_model.featureImportances.toArray()
    feat_imp = list(zip(FEATURE_COLS, importances))
    feat_imp.sort(key=lambda x: x[1], reverse=True)
    print("  Feature Importances (top features):")
    for name, imp in feat_imp:
        print("    {:<26s} {:.4f}".format(name, imp))

    print("[5/6] Saving results to HDFS:", HDFS_OUTPUT_DIR)

    predictions.select(
        "stn_code", "monitoring_location", "year", "water_body_type", "state_name",
        *FEATURE_COLS, "water_quality_label", "prediction", "probability",
    ).write.mode("overwrite").parquet(HDFS_OUTPUT_DIR + "/predictions")

    metrics = [
        ("AUC", round(auc, 4)),
        ("F1", round(f1, 4)),
        ("Accuracy", round(accuracy, 4)),
    ]
    metrics_df = spark.createDataFrame(metrics, ["metric", "value"])
    metrics_df.write.mode("overwrite").csv(HDFS_OUTPUT_DIR + "/metrics", header=True)

    fi_rdd = spark.sparkContext.parallelize(
        [(name, float(imp)) for name, imp in feat_imp]
    )
    fi_df = spark.createDataFrame(fi_rdd, ["feature", "importance"])
    fi_df.write.mode("overwrite").csv(HDFS_OUTPUT_DIR + "/feature_importances", header=True)

    print("[6/6] Done. Pipeline complete: Pig ETL -> Hive DDL -> Spark MLlib")
    spark.stop()


if __name__ == "__main__":
    main()