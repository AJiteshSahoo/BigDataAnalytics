"""
Unit and integration tests for Real-Time Fraud Detection Engine,
Distributed Kafka Bridge, and PySpark / Parquet Analytics.
"""

import os
import shutil
import time
import unittest
import pandas as pd

from engine import FraudScoringEngine
from kafka_bridge import DistributedKafkaBus, KafkaBusMode
from models import AnomalyType, RiskStatus, Transaction
from pipeline import FraudStreamingPipeline
from simulator import TransactionSimulator
from spark_analytics import SparkAnalyticsEngine


class TestFraudScoringEngine(unittest.TestCase):

    def setUp(self):
        self.engine = FraudScoringEngine(
            velocity_threshold=4,
            velocity_window_sec=60.0,
            drain_multiplier=5.0,
            decline_score_threshold=70.0,
            review_score_threshold=40.0,
        )

    def test_benign_transactions_approved(self):
        now = time.time()
        for i in range(3):
            txn = Transaction(
                transaction_id=f"TXN-BENIGN-{i}",
                user_id="USR-TEST-01",
                amount=45.0,
                timestamp=now + (i * 20.0),
                merchant_category="grocery",
                location="US",
                ip_address="192.168.1.10",
            )
            result = self.engine.evaluate_transaction(txn)
            self.assertEqual(result.status, RiskStatus.APPROVED)
            self.assertLess(result.risk_score, 40.0)

    def test_velocity_spike_detection(self):
        now = time.time()
        results = []
        for i in range(5):
            txn = Transaction(
                transaction_id=f"TXN-VEL-{i}",
                user_id="USR-VEL-01",
                amount=50.0,
                timestamp=now + (i * 1.0),
                merchant_category="electronics",
                location="US",
                ip_address="192.168.1.50",
            )
            res = self.engine.evaluate_transaction(txn)
            results.append(res)

        fourth_res = results[3]
        triggered_rules_4 = [r.rule_name for r in fourth_res.rules_triggered if r.triggered]
        self.assertIn("VELOCITY_SPIKE", triggered_rules_4)
        self.assertIn(fourth_res.status, (RiskStatus.REVIEW, RiskStatus.DECLINED))

    def test_geo_anomaly_impossible_travel(self):
        now = time.time()
        txn1 = Transaction(
            transaction_id="TXN-LEG1",
            user_id="USR-GEO-01",
            amount=30.0,
            timestamp=now,
            merchant_category="coffee",
            location="US",
            ip_address="192.168.1.20",
        )
        self.engine.evaluate_transaction(txn1)

        txn2 = Transaction(
            transaction_id="TXN-LEG2",
            user_id="USR-GEO-01",
            amount=120.0,
            timestamp=now + 2.0,
            merchant_category="luxury_retail",
            location="SG",
            ip_address="104.28.1.1",
        )
        res2 = self.engine.evaluate_transaction(txn2)
        triggered_rules = [r.rule_name for r in res2.rules_triggered if r.triggered]
        self.assertIn("GEO_ANOMALY", triggered_rules)
        self.assertGreaterEqual(res2.risk_score, 50.0)

    def test_high_value_drain(self):
        now = time.time()
        user_id = "USR-DRAIN-01"

        for i in range(3):
            txn = Transaction(
                transaction_id=f"TXN-BASE-{i}",
                user_id=user_id,
                amount=50.0,
                timestamp=now + (i * 10.0),
                merchant_category="grocery",
                location="US",
                ip_address="192.168.1.30",
            )
            self.engine.evaluate_transaction(txn)

        drain_txn = Transaction(
            transaction_id="TXN-DRAIN-ATTACK",
            user_id=user_id,
            amount=1200.0,
            timestamp=now + 40.0,
            merchant_category="crypto_exchange",
            location="US",
            ip_address="192.168.1.30",
        )
        res = self.engine.evaluate_transaction(drain_txn)
        triggered_rules = [r.rule_name for r in res.rules_triggered if r.triggered]
        self.assertIn("HIGH_VALUE_DRAIN", triggered_rules)
        self.assertEqual(res.status, RiskStatus.DECLINED)


class TestKafkaBridge(unittest.TestCase):

    def test_kafka_bus_fallback_and_publish(self):
        """Verify Kafka bridge falls back gracefully and handles publishing without crashes."""
        consumed = []
        bus = DistributedKafkaBus(
            bootstrap_servers="localhost:9092",
            on_message_consumed=lambda txn: consumed.append(txn)
        )
        self.assertIn(bus.mode, (KafkaBusMode.DISTRIBUTED_KAFKA, KafkaBusMode.LOCAL_MEMORY_FALLBACK))
        
        txn = Transaction(
            transaction_id="TXN-KAFKA-1",
            user_id="USR-KAFKA-01",
            amount=75.0,
            timestamp=time.time(),
            merchant_category="travel",
            location="DE",
            ip_address="127.0.0.1",
        )
        success = bus.publish(txn)
        self.assertTrue(success)
        self.assertGreaterEqual(bus.published_count, 1)

        status = bus.get_status()
        self.assertIn("partition_strategy", status)
        self.assertIn("user_id", status["partition_strategy"])
        bus.close()


class TestSparkAnalytics(unittest.TestCase):

    def setUp(self):
        self.test_parquet_dir = "data/test_transactions_parquet"
        self.spark_engine = SparkAnalyticsEngine(parquet_dir=self.test_parquet_dir)

    def tearDown(self):
        if os.path.exists(self.test_parquet_dir):
            shutil.rmtree(self.test_parquet_dir, ignore_errors=True)

    def test_parquet_sink_and_analytics(self):
        """Test partitioned Parquet writing, window analytics, and K-Means clustering."""
        records = []
        now = time.time()
        users = ["USR-A", "USR-B", "USR-C", "USR-D"]
        countries = ["US", "GB", "IN"]

        for i in range(40):
            uid = users[i % len(users)]
            loc = countries[i % len(countries)]
            amt = 50.0 + (i * 5.0)
            records.append({
                "transaction_id": f"TXN-SPARK-{i}",
                "user_id": uid,
                "amount": amt,
                "timestamp": now + (i * 2.0),
                "merchant_category": "electronics",
                "location": loc,
                "ip_address": "127.0.0.1",
                "risk_score": 10.0 + (i % 80),
                "status": "APPROVED" if (i % 5 != 0) else "DECLINED",
                "is_synthetic_fraud": False,
            })

        df = pd.DataFrame(records)

        # 1. Parquet sink
        path, count = self.spark_engine.write_to_parquet(df)
        self.assertEqual(count, len(df))
        self.assertTrue(os.path.exists(self.test_parquet_dir))

        # Verify partitioned folder structure
        subdirs = os.listdir(self.test_parquet_dir)
        self.assertTrue(any("location=" in s for s in subdirs))

        # 2. Window Analytics
        win_df = self.spark_engine.run_window_analytics(df)
        self.assertIn("inter_txn_delta_s", win_df.columns)
        self.assertIn("rolling_5_avg", win_df.columns)
        self.assertIn("spend_surge_ratio", win_df.columns)

        # 3. Regional SQL Aggregations
        reg_df = self.spark_engine.run_regional_sql_aggregations(df)
        self.assertIn("Country", reg_df.columns)
        self.assertIn("Total_Volume_USD", reg_df.columns)

        # 4. K-Means MLlib Clustering
        clustered_users, summary = self.spark_engine.run_mllib_kmeans_clustering(df)
        self.assertIn("Cluster_ID", clustered_users.columns)
        self.assertIn("Behavioral_Profile", clustered_users.columns)
        self.assertEqual(len(summary), 3)


class TestPipelineBatchJob(unittest.TestCase):

    def test_pipeline_spark_batch_execution(self):
        """Verify pipeline triggers spark batch job smoothly."""
        pipeline = FraudStreamingPipeline(tps=10.0, fraud_prob=0.1)
        res = pipeline.run_spark_batch_job(target_sample_size=30)
        
        self.assertEqual(res["status"], "COMPLETED")
        self.assertGreaterEqual(res["records_processed"], 30)
        self.assertIsNotNone(pipeline.latest_spark_results)


if __name__ == "__main__":
    unittest.main()
