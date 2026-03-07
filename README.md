# AML Graph Engine: Real-Time Fraud Detection Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.1-orange.svg)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.4.1-black.svg)
![Neo4j](https://img.shields.io/badge/Neo4j-Graph_DB-018bff.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B.svg)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED.svg)

## Project Overview
The AML Graph Engine is an end-to-end Data Engineering pipeline designed to detect Anti-Money Laundering (AML) patterns in real-time. By leveraging the power of streaming architectures and graph databases, this system ingests simulated financial transactions, masks Personally Identifiable Information (PII) on the fly, and applies complex graph traversal algorithms to identify suspicious activities such as Circular Laundering (Integration) and High-Value Anomalies (Placement).

## System Architecture

The architecture follows a robust, distributed data flow model:

1. **Data Generation (Producer):** A custom Python engine generating synthetic financial transactions (inspired by the PaySim dataset).
2. **Message Broker (Ingestion):** **Apache Kafka** acts as the central nervous system, decoupling the data producer from consumers and ensuring zero data loss during traffic spikes.
3. **Stream Processing (ETL):** **Apache Spark (Structured Streaming)** consumes micro-batches from Kafka. It enforces schema validation and applies `SHA-256` hashing to mask sensitive account numbers (Data Privacy compliance).
4. **Graph Storage (Sink):** Processed transactions are bulk-upserted into **Neo4j** using the `UNWIND` Cypher strategy. Neo4j's Index-Free Adjacency allows for millisecond-level relationship traversals.
5. **Analytical Dashboard (Serving):** A reactive **Streamlit** web application pushes optimized aggregated queries to Neo4j to render an Executive Command Center.

## Key Features
* **Real-time Streaming:** End-to-end latency in seconds from transaction generation to dashboard visualization.
* **Graph-based Pattern Matching:** Detects complex multi-hop circular money flows (e.g., `A -> B -> C -> A`) which are computationally expensive in traditional Relational Databases.
* **Data Security:** On-the-fly PII masking using SHA-256 hashing before data hits the persistent storage layer.
* **Executive Command Center:** Interactive UI with Plotly charts, outlier detection scatter plots, and critical alert taskforces.

## Tech Stack
* **Languages:** Python, Cypher (Graph Query Language)
* **Data Ingestion & Streaming:** Apache Kafka, Zookeeper
* **Distributed Processing:** PySpark
* **Database:** Neo4j (Graph Database)
* **Visualization:** Streamlit, Plotly
* **Infrastructure:** Docker, Docker Compose

## Getting Started

### Prerequisites
* Docker and Docker Compose
* Python 3.9 or higher
* Java 8 or 11 (required for PySpark)
* Git

### Installation & Setup

**1. Clone the repository:**
```
git clone [https://github.com/yourusername/aml-graph-engine.git](https://github.com/yourusername/aml-graph-engine.git)
cd aml-graph-engine
```

**2. Set up the virtual environment:**
```
python -m venv venv
# On Windows
venv\Scripts\activate
# On macOS/Linux
source venv/bin/activate

pip install -r requirements.txt
```

**3. Spin up the Infrastructure (Kafka & Neo4j):**
```
docker-compose -f infra/docker-compose.yml up -d
```

**4. Initialize the Graph Database:**

Open Neo4j Browser at http://localhost:7474 (User: neo4j, Pass: aml_graph_pass) and run the schema initialization:
```
Cypher
CREATE CONSTRAINT account_id_unique IF NOT EXISTS FOR (a:Account) REQUIRE a.account_id IS UNIQUE;
CREATE INDEX transaction_time_idx IF NOT EXISTS FOR ()-[r:TRANSFERRED_TO]-() ON (r.timestamp);
```

### Execution

To run the full pipeline, open four separate terminals (ensure the virtual environment is activated in each) and execute the following in order:

**Terminal 1: Start the Data Generator**
```
python src/generator/paysim_engine.py
```

**Terminal 2: Start the Spark Stream Processor**
```
python src/processors/stream_processor.py
```

**Terminal 3: Launch the Executive Dashboard**
```
streamlit run src/analytics/executive_dashboard.py
```

The dashboard will be available at http://localhost:8501.

## Analytical Insights
The Streamlit dashboard provides the following Executive Insights:

* **System Exposure:** Total transaction volume currently monitored.
* **Risk Distribution Treemap:** Visual hierarchy of transaction channels (e.g., Transfer, Cash Out) by total value.
* **Outlier Detection:** Scatter plot monitoring the last 500 transactions against dynamic alert thresholds.
* **Priority Taskforce Tables:** Automated flagging of high-value transactions (Placement stage) and closed-loop networks (Integration stage).
