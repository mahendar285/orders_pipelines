# Order-Line Revenue Pipeline | PySpark Data Engineering

### End-to-End Data Engineering Project | Python • PySpark • SQL • Data Quality • Performance Optimization

A PySpark-based batch data pipeline designed to transform raw e-commerce data into an analytics-ready `fact_order_line` dataset.

The project demonstrates practical data engineering concepts, including data ingestion, schema validation, data cleaning, transformation, duplicate handling, data quality checks, quarantine management, reconciliation, idempotent processing, and performance optimization.

Developed as a practical project aligned with the **YottaFlex AI Data Engineering / Forward Deployed Engineer challenge**.

---

## 🚀 Project Overview

In real-world data engineering environments, raw data often contains missing values, duplicate records, invalid formats, inconsistent data types, and incorrect references.

This project addresses these challenges by implementing an end-to-end PySpark pipeline that:

- Ingests multiple raw CSV datasets.
- Validates schemas and data types.
- Identifies missing, invalid, and duplicate records.
- Separates valid and rejected records.
- Applies business rules and transformations.
- Joins customer, product, and order information.
- Generates an analytics-ready fact table.
- Performs data quality checks and reconciliation.
- Compares baseline and optimized Spark execution strategies.

**Main objective:** Build a reliable, traceable, repeatable, and performance-conscious data processing pipeline.

---

## 🏗️ Pipeline Architecture

```text
        Raw CSV Datasets
               |
               v
        Data Ingestion
               |
               v
        Schema Validation
               |
               v
        Data Type Conversion
               |
               v
        Data Quality Checks
               |
               v
       Duplicate Detection
               |
               v
    Referential Integrity Checks
               |
               v
      Data Transformation
               |
          +----+----+
          |         |
          v         v
     Valid Data   Rejected Data
          |         |
          v         v
    Curated Fact  Quarantine
       Table       Dataset
          |
          v
   Quality & Reconciliation
          |
          v
   Run Summary and Metrics
```

---

## 📂 Dataset Description

The project uses four reproducible synthetic datasets:

| Dataset | Description | Primary Grain |
|---|---|---|
| Customers | Customer information and versions | customer_id |
| Products | Product and SKU information | sku |
| Orders | Order-level information | order_id |
| Order Items | Individual order-line details | order_id, line_no |

**Final Output:** `fact_order_line`

The target table retains the order-item grain and enriches each order line with customer and product attributes, revenue measures, and additional analytical fields.

Synthetic data is generated using a fixed random seed, with separate expected labels for validating rejected records.

---

## 🛠️ Technology Stack

| Category | Technologies |
|---|---|
| Programming | Python |
| Big Data Processing | Apache PySpark |
| Database & Querying | SQL |
| Data Processing | Pandas |
| Data Storage | CSV, Parquet |
| Testing | Pytest |
| Configuration | YAML |
| Development Tools | VS Code, Git, GitHub |

---

## ⚙️ Key Features

### 1. Data Ingestion
- Reads customer, product, order, and order-item CSV files.
- Handles malformed records.
- Validates incoming dataset schemas.
- Supports configurable input and output paths.

### 2. Data Validation & Quality
- Checks missing required fields.
- Validates data types and formats.
- Identifies invalid quantities and prices.
- Checks currency, email, and country-code formats.
- Detects duplicate and conflicting records.
- Validates parent-child relationships.

### 3. Data Transformation
- Joins multiple datasets using business keys.
- Calculates revenue and margin-related measures.
- Applies business rules.
- Handles late-arriving records.
- Produces an analytics-ready fact table.

### 4. Quarantine Management
Invalid records are stored separately instead of being silently discarded.

The quarantine dataset captures:
- Source table
- Rejection reason
- Record key
- Source file
- Original record payload
- Run ID

This makes rejected records traceable and easier to investigate.

### 5. Idempotency & Reliability
- Supports safe reruns using run IDs.
- Prevents duplicate processing during repeated runs.
- Supports date-based backfills.
- Replaces only the relevant output partitions.
- Provides full-refresh functionality.

### 6. Performance Optimization

The project compares two Spark execution strategies.

**Baseline Strategy**
- AQE disabled.
- Broadcast joins disabled.
- Additional repartitioning.
- Python UDF for revenue-band calculation.
- Unpartitioned output.

**Optimized Strategy**
- Adaptive Query Execution enabled.
- Broadcast joins for smaller dimensions.
- Early filtering and column selection.
- Native Spark expressions instead of Python UDFs.
- Date-partitioned output.

Both strategies are designed to produce equivalent business results, with checksum validation before comparing execution timings.

### 7. Reconciliation & Monitoring
The pipeline includes checks for:
- Source and target record counts.
- Daily revenue totals.
- Rejected records by reason.
- Duplicate business keys.
- Revenue by category or band.
- Invalid revenue from cancelled or refunded records.
- Late-arriving records.
- Target table grain.

---

## 🧪 Testing

The project includes a test suite covering 32 test cases.

Test scenarios include:

- Normal data processing.
- Empty input datasets.
- Malformed records.
- Missing required fields.
- Invalid data types.
- Exact and conflicting duplicates.
- Invalid reference keys.
- Rejected parent records.
- Late-arriving orders.
- Idempotent reruns.
- Date-based backfills.
- Data reconciliation.

Tests are designed to run locally using a Spark session without requiring a cluster.

---

## 📊 Project Structure

```text
orders-pipeline/
│
├── conf/
│   └── pipeline.yaml
│
├── data/
│   ├── raw/
│   ├── reference/
│   ├── curated/
│   ├── quarantine/
│   └── runs/
│
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   ├── design_tradeoffs.md
│   ├── limitations_and_next_steps.md
│   └── run_evidence.md
│
├── sql/
│   └── reconciliation.sql
│
├── src/
│   └── pipeline/
│       ├── config.py
│       ├── schemas.py
│       ├── session.py
│       ├── validate.py
│       ├── transform.py
│       ├── loader.py
│       ├── quality.py
│       ├── metrics.py
│       ├── generator.py
│       ├── benchmark.py
│       ├── reconcile.py
│       ├── runner.py
│       └── cli.py
│
├── tests/
├── requirements.txt
├── Makefile
└── README.md
```

---

## 💻 Installation & Setup

### Prerequisites

- Python 3.10+
- JDK 11 or 17
- Apache Spark through PySpark
- Git

### Step 1: Clone the Repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd orders-pipeline
```

### Step 2: Create a Virtual Environment

```bash
python -m venv .venv
```

Activate it:

**Windows**
```bash
.venv\Scripts\activate
```

**Linux / macOS**
```bash
source .venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Generate Sample Data

```bash
make sample
```

### Step 5: Run Tests

```bash
make test
```

### Step 6: Execute the Pipeline

```bash
make run
```

### Step 7: Run Reconciliation

```bash
make reconcile
```

### Step 8: Run Performance Benchmark

```bash
make benchmark
```

---

## 📁 Pipeline Outputs

The pipeline generates the following outputs:

| Output | Purpose |
|---|---|
| `data/curated/fact_order_line/` | Final analytics-ready Parquet dataset |
| `data/quarantine/` | Rejected records and rejection reasons |
| `data/runs/` | Run summaries and operational metrics |
| `data/reference/` | Expected labels for validation |
| `data/raw/` | Synthetic source datasets |

These outputs support data quality analysis, reconciliation, troubleshooting, and performance evaluation.

---

## 📈 Performance Evaluation

The project includes a benchmark module to compare baseline and optimized Spark execution.

Performance evaluation considers:

- Execution time.
- Stage timings.
- Shuffle partition count.
- Join strategy.
- Execution plan.
- Output checksum.

The benchmark validates output equivalence before reporting timing differences.

Actual performance results should be generated by running the benchmark in the target environment.

---

## 🔍 Engineering Concepts Demonstrated

- End-to-end ETL pipeline development.
- Distributed data processing with PySpark.
- Schema enforcement and data validation.
- Data quality and error handling.
- Referential integrity.
- Duplicate detection and resolution.
- Idempotent data processing.
- Retry and backfill strategies.
- Data reconciliation.
- Logging and operational metrics.
- Spark performance optimization.
- Automated testing.
- Modular and configurable pipeline design.

---

## ⚠️ Limitations & Future Improvements

- Introduce Delta Lake or Apache Iceberg for transactional data management.
- Add SCD Type 2 dimension history.
- Implement streaming data ingestion.
- Add cloud object storage integration.
- Improve performance monitoring with Spark UI metrics.
- Extend the pipeline for larger production datasets.

---

## 🎯 Project Objective

This project demonstrates my interest in solving practical data engineering problems through reliable data processing, validation, optimization, and analytical data publishing.

It reflects my learning and hands-on work in Python, PySpark, SQL, data quality, and end-to-end pipeline development.

---

## 👨‍💻 Author

**K. Mahender Reddy**

B.Tech – Computer Science and Engineering (AI & ML)  
Malla Reddy University

- 💻 GitHub: https://github.com/mahendar285
- 🌍 Portfolio: https://mahender-reddy-portfolio.base44.app
- 📧 Email: kmahendarredd2005@gmail.com

---

⭐ If you find this project useful, feel free to explore the repository and connect with me.
