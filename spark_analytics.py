"""
Distributed PySpark SQL, Parquet Data Lake & MLlib Unsupervised Anomaly Engine.

Features:
1. Apache Parquet Data Lake Sink:
   Stores historical transaction batches into a partitioned columnar data lake
   (data/transactions_parquet) partitioned by 'location' (ISO country).
2. Distributed Window Analytics:
   Uses PySpark Window specification:
     Window.partitionBy("user_id").orderBy("timestamp").rowsBetween(-4, 0)
   with F.lag() to compute inter-transaction time deltas, rolling 5-transaction
   average spend, and spend surge ratios.
3. Spark SQL Aggregations:
   Groups by location and merchant_category to compute regional fraud rates,
   total intercepted volumes, and max surge multipliers.
4. Spark MLlib Unsupervised Anomaly Clustering:
   Extracts behavioral feature vectors per user_id:
     [txn_count, avg_amount, max_amount, std_amount, avg_risk_score]
   Assembles and standardizes vectors with VectorAssembler and StandardScaler,
   training a distributed KMeans (k=3) model to classify:
     - Normal Baseline
     - High-Velocity Spike / Bot
     - Whale Drain Outliers
5. Zero-Crash Graceful Degradation:
   If no JVM / Spark cluster is reachable locally, transparently executes
   the identical columnar Parquet partitioning via PyArrow and analytics via
   optimized Pandas and Scikit-Learn KMeans.
"""

import os
import shutil
import time
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd


class SparkAnalyticsEngine:
    """
    Unified Big Data Engine supporting Distributed PySpark and zero-crash PyArrow/Scikit-Learn fallback.
    """

    def __init__(
        self,
        master_url: Optional[str] = None,
        parquet_dir: str = "data/transactions_parquet",
    ):
        self.master_url = master_url or os.environ.get("SPARK_MASTER_URL", "local[*]")
        self.parquet_dir = parquet_dir
        self.spark_session = None
        self.mode = "PARQUET_ARROW_FALLBACK (Zero-Crash Mode: No JVM detected)"
        self.is_spark_available = False

        self._init_spark()

    def _init_spark(self) -> None:
        """Attempt to start or connect to an Apache Spark cluster."""
        # Fast pre-flight check: If no Java/JVM is installed on the host, don't stall on PySpark subprocess
        has_java = shutil.which("java") is not None
        if not has_java:
            self.spark_session = None
            self.is_spark_available = False
            self.mode = "PARQUET_ARROW_FALLBACK (Zero-Crash Mode: No JVM detected)"
            return

        try:
            from pyspark.sql import SparkSession
            spark = (
                SparkSession.builder
                .appName("StreamGuard-Analytics")
                .master(self.master_url)
                .config("spark.driver.memory", "1g")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.ui.enabled", "false")
                .getOrCreate()
            )
            spark.range(1).count()
            self.spark_session = spark
            self.is_spark_available = True
            self.mode = f"DISTRIBUTED_PYSPARK (Master: {self.master_url})"
        except Exception:
            self.spark_session = None
            self.is_spark_available = False
            self.mode = "PARQUET_ARROW_FALLBACK (Zero-Crash Mode: No JVM detected)"

    def write_to_parquet(self, df: pd.DataFrame) -> Tuple[str, int]:
        """
        Saves transaction batch to Apache Parquet data lake partitioned by 'location'.
        """
        if df.empty:
            return self.parquet_dir, 0

        # Ensure directory exists
        os.makedirs(self.parquet_dir, exist_ok=True)

        if self.is_spark_available and self.spark_session:
            try:
                spark_df = self.spark_session.createDataFrame(df)
                spark_df.write.mode("append").partitionBy("location").parquet(self.parquet_dir)
                return self.parquet_dir, len(df)
            except Exception:
                pass

        # Zero-Crash Native Columnar Fallback via PyArrow
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(
            table,
            root_path=self.parquet_dir,
            partition_cols=["location"],
            existing_data_behavior="overwrite_or_ignore",
        )
        return self.parquet_dir, len(df)

    def run_window_analytics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Distributed Window Analytics:
        Computes inter-transaction time deltas, rolling 5-txn averages, and spend surge ratios.
        """
        if df.empty:
            return pd.DataFrame()

        if self.is_spark_available and self.spark_session:
            try:
                from pyspark.sql import functions as F
                from pyspark.sql.window import Window

                spark_df = self.spark_session.createDataFrame(df)
                win_order = Window.partitionBy("user_id").orderBy("timestamp")
                win_rolling5 = Window.partitionBy("user_id").orderBy("timestamp").rowsBetween(-4, 0)

                analyzed_df = (
                    spark_df
                    .withColumn("prev_ts", F.lag("timestamp", 1).over(win_order))
                    .withColumn(
                        "inter_txn_delta_s",
                        F.coalesce(F.col("timestamp") - F.col("prev_ts"), F.lit(0.0))
                    )
                    .withColumn("rolling_5_avg", F.avg("amount").over(win_rolling5))
                    .withColumn(
                        "spend_surge_ratio",
                        F.round(F.col("amount") / F.coalesce(F.col("rolling_5_avg"), F.lit(1.0)), 2)
                    )
                )
                return analyzed_df.toPandas()
            except Exception:
                pass

        # Zero-Crash Vectorized Window Fallback via Pandas
        res = df.sort_values(by=["user_id", "timestamp"]).copy()
        res["prev_ts"] = res.groupby("user_id")["timestamp"].shift(1)
        res["inter_txn_delta_s"] = (res["timestamp"] - res["prev_ts"]).fillna(0.0).round(2)
        res["rolling_5_avg"] = (
            res.groupby("user_id")["amount"]
            .transform(lambda s: s.rolling(5, min_periods=1).mean())
            .round(2)
        )
        res["spend_surge_ratio"] = (res["amount"] / res["rolling_5_avg"]).fillna(1.0).round(2)
        return res

    def run_regional_sql_aggregations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Distributed SQL Aggregations:
        Groups by location and merchant_category to compute aggregate regional metrics.
        """
        if df.empty:
            return pd.DataFrame()

        if self.is_spark_available and self.spark_session:
            try:
                spark_df = self.spark_session.createDataFrame(df)
                spark_df.createOrReplaceTempView("transactions_view")
                query = """
                    SELECT
                        location AS Country,
                        merchant_category AS Category,
                        COUNT(transaction_id) AS Total_Txns,
                        ROUND(SUM(amount), 2) AS Total_Volume_USD,
                        SUM(CASE WHEN status = 'DECLINED' THEN 1 ELSE 0 END) AS Blocked_Fraud_Txns,
                        ROUND(SUM(CASE WHEN status = 'DECLINED' THEN 1.0 ELSE 0.0 END) * 100.0 / COUNT(transaction_id), 1) AS Regional_Fraud_Rate_Pct,
                        ROUND(MAX(amount), 2) AS Max_Single_Amount,
                        ROUND(AVG(amount), 2) AS Avg_Amount_USD
                    FROM transactions_view
                    GROUP BY location, merchant_category
                    ORDER BY Total_Volume_USD DESC
                """
                return self.spark_session.sql(query).toPandas()
            except Exception:
                pass

        # Zero-Crash SQL Fallback via Pandas Aggregation
        agg = (
            df.groupby(["location", "merchant_category"])
            .apply(
                lambda g: pd.Series({
                    "Total_Txns": len(g),
                    "Total_Volume_USD": round(g["amount"].sum(), 2),
                    "Blocked_Fraud_Txns": int((g["status"] == "DECLINED").sum()),
                    "Regional_Fraud_Rate_Pct": round(((g["status"] == "DECLINED").sum() / len(g)) * 100.0, 1),
                    "Max_Single_Amount": round(g["amount"].max(), 2),
                    "Avg_Amount_USD": round(g["amount"].mean(), 2),
                }),
                include_groups=False
            )
            .reset_index()
            .rename(columns={"location": "Country", "merchant_category": "Category"})
            .sort_values(by="Total_Volume_USD", ascending=False)
        )
        return agg

    def run_mllib_kmeans_clustering(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Distributed Spark MLlib K-Means Unsupervised Anomaly Clustering:
        Clusters user behavioral profiles into 3 distinct cohorts:
        1. Normal Baseline User
        2. High-Velocity Spike / Bot
        3. Whale Drain Outlier
        """
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        # Step 1: Feature Aggregation per user_id
        user_features = (
            df.groupby("user_id")
            .agg(
                txn_count=("transaction_id", "count"),
                avg_amount=("amount", "mean"),
                max_amount=("amount", "max"),
                std_amount=("amount", lambda s: s.std() if len(s) > 1 else 0.0),
                avg_risk_score=("risk_score", "mean"),
            )
            .reset_index()
        )
        user_features["std_amount"] = user_features["std_amount"].fillna(0.0)

        # Ensure at least 3 samples for k=3
        if len(user_features) < 3:
            user_features["Cluster_ID"] = 0
            user_features["Behavioral_Profile"] = "Normal Baseline User"
            return user_features, pd.DataFrame()

        if self.is_spark_available and self.spark_session:
            try:
                from pyspark.ml.clustering import KMeans
                from pyspark.ml.feature import StandardScaler, VectorAssembler

                spark_feat_df = self.spark_session.createDataFrame(user_features)
                feature_cols = ["txn_count", "avg_amount", "max_amount", "std_amount", "avg_risk_score"]
                
                assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
                assembled = assembler.transform(spark_feat_df)

                scaler = StandardScaler(inputCol="raw_features", outputCol="scaled_features", withStd=True, withMean=True)
                scaler_model = scaler.fit(assembled)
                scaled = scaler_model.transform(assembled)

                kmeans = KMeans(k=3, featuresCol="scaled_features", predictionCol="Cluster_ID", seed=42)
                model = kmeans.fit(scaled)
                clustered = model.transform(scaled).toPandas()

                # Add labels
                return self._label_clusters(clustered)
            except Exception:
                pass

        # Zero-Crash Machine Learning Fallback via Scikit-Learn KMeans
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import StandardScaler

        feat_cols = ["txn_count", "avg_amount", "max_amount", "std_amount", "avg_risk_score"]
        X = user_features[feat_cols].values
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        km = KMeans(n_clusters=3, random_state=42, n_init=10)
        user_features["Cluster_ID"] = km.fit_predict(X_scaled)

        return self._label_clusters(user_features)

    def _label_clusters(self, clustered_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Assign human-readable semantic fraud labels based on cluster center characteristics."""
        summary = (
            clustered_df.groupby("Cluster_ID")
            .agg(
                User_Count=("user_id", "count"),
                Mean_Txn_Count=("txn_count", "mean"),
                Mean_Amount=("avg_amount", "mean"),
                Max_Single_Spend=("max_amount", "max"),
                Mean_Risk_Score=("avg_risk_score", "mean"),
            )
            .reset_index()
        )

        # Labeling heuristic
        label_map = {}
        for _, row in summary.iterrows():
            cid = int(row["Cluster_ID"])
            if row["Mean_Txn_Count"] >= summary["Mean_Txn_Count"].max() and row["Mean_Txn_Count"] > 4:
                label_map[cid] = "⚡ High-Velocity Spike / Bot"
            elif row["Max_Single_Spend"] >= summary["Max_Single_Spend"].max() and row["Max_Single_Spend"] > 400:
                label_map[cid] = "💰 Whale Drain Outlier"
            else:
                label_map[cid] = "🟢 Normal Baseline User"

        clustered_df["Behavioral_Profile"] = clustered_df["Cluster_ID"].map(label_map).fillna("🟢 Normal Baseline User")
        summary["Behavioral_Profile"] = summary["Cluster_ID"].map(label_map).fillna("🟢 Normal Baseline User")

        return clustered_df, summary
