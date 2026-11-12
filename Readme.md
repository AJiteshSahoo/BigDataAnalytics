project is made by : Aakash ( 231210001 ) , Agaz ( 231210005 ) , Ajitesh ( 231210007) 

# StreamGuard: Distributed Real-Time Payment Fraud Detection & Big Data Analytics Engine

StreamGuard is a production-grade Distributed Systems and Big Data Analytics project combining **Apache Kafka (KRaft Mode)**, **Apache Spark (1 Master + 2 Workers)**, **Partitioned Apache Parquet Data Lakes**, **PySpark MLlib Unsupervised K-Means Clustering**, and a **Sub-Millisecond Sliding-Window Stream Scoring Engine**.

It features a **Zero-Crash Graceful Degradation Architecture**: when Docker and Java are active, it operates across distributed Kafka and Spark cluster nodes; if run on a barebones machine without Docker/JVM, it transparently falls back to high-speed in-memory queues, PyArrow columnar Parquet sinks, and Scikit-Learn K-Means without crashing.

---

##  Project Structure

```text
StreamGuard/
├── docker-compose.yml       # Multi-node cluster config: Kafka Broker + 1 Spark Master + 2 Spark Workers
├── models.py                # Core domain schemas (Transaction, RiskResult, RuleEvaluation, UserProfile)
├── engine.py                # Stateful Sliding-Window Fraud Scoring Engine (<0.05 ms latency SLA)
├── simulator.py             # Synthetic payment stream producer & live fraud attack injectors
├── kafka_bridge.py          # Distributed Partitioned Kafka Bus with automatic in-memory fallback
├── spark_analytics.py       # PySpark SQL, Partitioned Parquet Sink, Windowing & MLlib K-Means Engine
├── pipeline.py              # Thread-safe orchestrator connecting Producer, Kafka, Scoring & Spark
├── app.py                   # Interactive Streamlit web dashboard & live operations center
├── run_cli.py               # Rich terminal-based live streaming CLI monitor
├── test_fraud_detection.py  # Automated unit & integration test suite (7 verification tests)
├── requirements.txt         # Python package dependencies
└── README.md                # Project documentation, architecture & setup guide
```

---


## Description of Architecture Components (1–3 Lines Each)

1. **Transaction Simulator (`simulator.py`):** Generates continuous realistic baseline payment traffic across 50 user profiles while allowing automated or manual one-click injection of anomalous fraud attacks (Velocity Bursts, Impossible Geo-Hops, and Whale Drains).
2. **Apache Kafka Broker (`docker-compose.yml` & `kafka_bridge.py`):** Acts as the distributed, fault-tolerant streaming ingestion bus running in ZooKeeper-less KRaft mode. It uses deterministic key-based hashing ($\text{hash}(\text{user\_id}) \pmod 4$) so all events for a specific user land in the exact same partition in strict FIFO order.
3. **Keyed Sliding-Window State Store (`engine.py`):** Maintains an in-memory per-user state partition (`UserWindowState`) with a 60-second sliding window buffer. It lazily evicts expired transactions upon new event arrivals and tracks cumulative baseline spending averages and standard deviations.
4. **Multi-Factor Risk Scoring Engine (`engine.py`):** Evaluates incoming transactions in sub-millisecond time ($<0.05\text{ ms}$) by combining deterministic rules (sliding-window velocity, Haversine great-circle geo-velocity) with statistical anomaly metrics ($Z$-score outlier detection) to assign a risk score from $0$ to $100$.
5. **Partitioned Apache Parquet Data Lake (`spark_analytics.py`):** Persists evaluated transaction batches into a columnar binary storage layout partitioned on disk by country (`location=US`, `location=GB`, etc.), enabling high compression and fast partition pruning during queries.
6. **Apache Spark Master & 2 Worker Nodes (`docker-compose.yml` & `spark_analytics.py`):** Forms the distributed batch computation cluster (1 Master node coordinating 2 Worker containers). It executes parallelized PySpark Window specifications (`Window.partitionBy("user_id")`), regional Spark SQL aggregations, and distributed machine learning.
7. **Spark MLlib K-Means Clustering (`spark_analytics.py`):** Extracts 5-dimensional behavioral feature vectors per user, normalizes them using `StandardScaler`, and runs unsupervised $K$-Means clustering ($k=3$) to segment users into *Normal Baseline*, *High-Velocity Bot*, and *Whale Drain Outlier* cohorts.
8. **Streamlit Web Dashboard & CLI Monitor (`app.py` & `run_cli.py`):** Provides the interactive frontend interface featuring live animated KPI cards, real-time Plotly anomaly charts, an explainable Fraud Alert Center, and one-click triggers for Spark batch jobs and fraud simulations.


##  Technologies Used

 Category                                         Technology / Library                                             Purpose in Project |
 
 **Programming Language**                          Python 3.9+ |                                                   Core implementation language for streaming, scoring, and analytics 
 **Stream Messaging**                              Apache Kafka 7.6.0 (KRaft) & `kafka-python-ng`                  Distributed partitioned message broker and producer/consumer client 
 **Distributed Compute**                           Apache Spark 3.5+ (`pyspark`)                                   Distributed cluster processing (1 Master + 2 Worker nodes) & Spark SQL 
 **Machine Learning**                              PySpark MLlib & `scikit-learn`                                  Unsupervised $K$-Means clustering ($k=3$) & `StandardScaler` normalization 
 **Data Lake Storage**                             Apache Parquet & `pyarrow`                                      Columnar storage format partitioned by country (`location=*`) 
 **Web Frontend UI**                               Streamlit (`streamlit`) & Plotly (`plotly`)                     Real-time interactive web dashboard, alert center, and live charts 
 **Terminal UI**                                   `rich`                                                          Live color-coded CLI streaming dashboard (`run_cli.py`) 
 **Containerization**                              Docker & Docker Compose                                         Orchestrating the multi-node Kafka + Spark Master + 2 Spark Workers setup 



##  Prerequisites & What Needs to be Installed

### 1. Required Software (Minimum to Run)
* **Python 3.9 to 3.12** installed on your system.
* **pip** (Python package manager).

### 2. Optional Software (For Full Distributed Multi-Node Cluster Mode)
* **Docker Desktop** (with `docker compose` CLI) — Required if you want to spin up the actual containerized Apache Kafka broker, Spark Master node, and 2 Spark Worker nodes.
* **Java 11 or 17 (OpenJDK)** — Required on the host machine only if running local PySpark driver processes outside Docker.
*(Note: If Docker or Java are not installed, StreamGuard's Zero-Crash Fallback Mode automatically activates so the project still runs 100% smoothly during your demo!)*

---

##  Step-by-Step Installation & Execution Guide

### Step 1: Install Python Dependencies
Open a terminal in the project folder and install the required packages:
```bash
pip install -r requirements.txt
pip install scikit-learn
```

### Step 2: Start the Multi-Node Cluster (1 Master + 2 Workers + Kafka)
If Docker Desktop is running, start the distributed cluster in the background:
```bash
docker compose up -d
```
* **Verify Spark Master & 2 Workers UI:** Open `http://localhost:8080` in your browser to show evaluators the live Spark Master node and the 2 connected Worker nodes (`spark-worker-1` and `spark-worker-2`).
* **Kafka Broker Port:** Active on `localhost:9092`.

### Step 3: Launch the Streamlit Web Dashboard
Run the following command to start the interactive frontend:
```bash
python -m streamlit run app.py
```
* The dashboard will automatically open in your browser at **`http://localhost:8501`**.

### Step 4: (Optional) Run the Automated Test Suite or CLI Monitor
* **To verify all 7 unit and integration tests:**
  ```bash
  python test_fraud_detection.py
  ```
* **To launch the terminal CLI live monitor:**
  ```bash
  python run_cli.py
  ```
