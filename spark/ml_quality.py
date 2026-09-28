#!/usr/bin/env python3
"""
============================================================================
AquaKwal: Water Quality Analytics Pipeline
File: spark/ml_quality.py
Purpose: Read the Hive/Pig-cleaned, enriched water quality dataset from HDFS
         and train a Spark MLlib classifier to predict water quality
         (SAFE vs UNSAFE) from physico-chemical features.
           - HDFS -> Spark (distributed load)
           - Spark MLlib (feature assembly, scaling, RandomForest)
           - Model evaluation (accuracy, confusion matrix, F1)
============================================================================
"""

import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler,
    StandardScaler,
    StringIndexer,
    SQLTransformer,
)
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HDFS_CLEAN_PATH = "/data/clean/water_quality_enriched"
HDFS_OUTPUT_DIR = "/data/output/ml_results"


def main():
    spark = (
        SparkSession.builder.appName("AquaKwal-WaterQuality-Classifier")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # -----------------------------------------------------------------------
    # 1. LOAD cleaned enriched data from HDFS (written by Pig ETL stage)
    # -----------------------------------------------------------------------
    print("[1/5] Loading cleaned enriched data from HDFS:", HDFS_CLEAN_PATH)

    df = spark.read.csv(HDFS_CLEAN_PATH, header=False, inferSchema=True)

    # Rename columns to semantic names (Pig writes positionally)
    col_names = [
        "site_id", "sample_date", "ph", "temperature", "dissolved_oxygen",
        "conductivity", "turbidity", "nitrate", "sulfate", "latitude",
        "longitude", "water_quality_label", "watershed_name", "county",
        "state", "elevation",
    ]
    df = df.toDF(*col_names)

    # Cast numerics explicitly
    numeric_cols = [
        "ph", "temperature", "dissolved_oxygen", "conductivity",
        "turbidity", "nitrate", "sulfate", "latitude", "longitude", "elevation",
    ]
    for c in numeric_cols:
        df = df.withColumn(c, df[c].cast(FloatType()))

    # Drop any remaining nulls in feature / label columns
    feature_cols = numeric_cols + ["elevation"]
    label_col = "water_quality_label"
    df = df.na.drop(subset=feature_cols + [label_col])

    print("  Rows loaded:", df.count())
    df.groupBy(label_col).count().show()

    # -----------------------------------------------------------------------
    # 2. LABEL INDEXING — String "SAFE"/"UNSAFE" -> numeric 0/1
    # -----------------------------------------------------------------------
    label_indexer = StringIndexer(
        inputCol=label_col, outputCol="label", handleInvalid="skip"
    )

    # -----------------------------------------------------------------------
    # 3. FEATURE ENGINEERING — assemble numeric features into a vector,
    #    then scale for algorithms sensitive to magnitude (LogReg).
    # -----------------------------------------------------------------------
    assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="features_raw"
    )

    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features", withMean=True, withStd=True
    )

    # -----------------------------------------------------------------------
    # 4. MODEL SELECTION — RandomForest (non-linear, robust to feature scale)
    #    and LogisticRegression (linear baseline).  We use 5-fold CV with a
    #    small grid.
    # -----------------------------------------------------------------------
    rf = RandomForestClassifier(
        labelCol="label", featuresCol="features", predictionCol="prediction",
        numTrees=50, maxDepth=6, seed=42,
    )

    lr = LogisticRegression(
        labelCol="label", featuresCol="features", predictionCol="prediction",
        maxIter=100, regParam=0.3, elasticNetParam=0.8, seed=42,
    )

    # Choose the RF classifier as the primary model for the pipeline.
    classifier = rf

    # -----------------------------------------------------------------------
    # 5. BUILD PIPELINE
    # -----------------------------------------------------------------------
    pipeline = Pipeline(stages=[label_indexer, assembler, scaler, classifier])

    # -----------------------------------------------------------------------
    # 6. TRAIN / TEST SPLIT
    # -----------------------------------------------------------------------
    seed = 42
    train, test = df.randomSplit([0.8, 0.2], seed=seed)

    print("[2/5] Training rows:", train.count(), "  Test rows:", test.count())

    # -----------------------------------------------------------------------
    # 7. CROSS-VALIDATED HYPERPARAMETER SEARCH
    # -----------------------------------------------------------------------
    param_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [30, 50])
        .addGrid(rf.maxDepth, [4, 6])
        .build()
    )

    evaluator = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3,
        seed=seed,
    )

    # -----------------------------------------------------------------------
    # 8. FIT & PREDICT
    # -----------------------------------------------------------------------
    print("[3/5] Training model (cross-validated)...")
    cv_model = cv.fit(train)

    print("[4/5] Evaluating on test set...")
    predictions = cv_model.transform(test)

    # Binary metrics
    auc = evaluator.evaluate(predictions)
    print("  AUC (areaUnderROC) on test set: {:.4f}".format(auc))

    # Multiclass metrics
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

    # Confusion matrix
    print("  Confusion Matrix:")
    predictions.select("label", "prediction").distinct().orderBy("label", "prediction").show()

    # -----------------------------------------------------------------------
    # 9. FEATURE IMPORTANCES (Random Forest)
    # -----------------------------------------------------------------------
    best_model = cv_model.bestModel
    stages = best_model.stages
    rf_model = stages[-1]  # last stage of the pipeline

    importances = rf_model.featureImportances.toArray()
    feat_imp = list(zip(feature_cols, importances))
    feat_imp.sort(key=lambda x: x[1], reverse=True)
    print("  Feature Importances (top features):")
    for name, imp in feat_imp:
        print("    {:<18s} {:.4f}".format(name, imp))

    # -----------------------------------------------------------------------
    # 10. PERSIST RESULTS to HDFS
    # -----------------------------------------------------------------------
    print("[5/5] Saving predictions and metrics to HDFS:", HDFS_OUTPUT_DIR)

    # Predictions
    predictions.select(
        "site_id", "sample_date", "state", "watershed_name",
        "ph", "dissolved_oxygen", "temperature", "nitrate", "sulfate",
        "features", "label", "prediction", "probability",
    ).write.mode("overwrite").parquet(HDFS_OUTPUT_DIR + "/predictions")

    # Summary metrics
    metrics = [
        ("AUC", round(auc, 4)),
        ("F1", round(f1, 4)),
        ("Accuracy", round(accuracy, 4)),
    ]
    metrics_df = spark.createDataFrame(metrics, ["metric", "value"])
    metrics_df.write.mode("overwrite").csv(HDFS_OUTPUT_DIR + "/metrics", header=True)

    # Feature importances
    fi_rdd = spark.sparkContext.parallelize(
        [(name, float(imp)) for name, imp in feat_imp]
    )
    fi_df = spark.createDataFrame(fi_rdd, ["feature", "importance"])
    fi_df.write.mode("overwrite").csv(HDFS_OUTPUT_DIR + "/feature_importances", header=True)

    print("Done. Pipeline complete: Pig ETL -> Hive DDL -> Spark MLlib")
    spark.stop()


if __name__ == "__main__":
    main()
