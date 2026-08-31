# ⚡ Apache Spark Core — Simplified Guide

> **Apache Spark** is an open-source, distributed, in-memory data processing engine designed for fast and large-scale data analytics.

---

## 📌 Quick Summary

| Topic | Key Concept | Simple Summary |
|---|---|---|
| **Spark Architecture** | Driver & Executors | Driver plans & orchestrates; Executors run tasks on data partitions. |
| **SparkSession** | Unified Entry Point | Single gateway to access all Spark features (DataFrames, SQL, Streaming). |
| **RDDs vs DataFrames** | Low-Level vs High-Level | RDDs = raw objects without optimization; DataFrames = structured tables with Catalyst optimization. |
| **Lazy Evaluation** | Deferred Execution | Spark builds an execution plan (DAG) and computes ONLY when an **Action** is triggered. |

---

## 1. 🏗️ Spark Architecture (Driver & Executors)

Spark uses a **Master-Worker (Master-Slave)** architecture to process data across a cluster.

### 🧩 Component Overview

```
                    ┌──────────────────────────────────────┐
                    │             DRIVER NODE              │
                    │                                      │
                    │   ┌──────────────────────────────┐   │
                    │   │         SparkSession         │   │
                    │   └──────────────┬───────────────┘   │
                    │                  │                   │
                    │      DAG Scheduler / Task Scheduler  │
                    └──────────────────┬───────────────────┘
                                       │
                                       ▼
                    ┌──────────────────────────────────────┐
                    │           CLUSTER MANAGER            │
                    │    (YARN / Kubernetes / Standalone)  │
                    └──────┬───────────────────────┬───────┘
                           │                       │
           ┌───────────────┴───────┐       ┌───────┴───────────────┐
           ▼                       ▼       ▼                       ▼
┌─────────────────────┐ ┌─────────────────────┐ ┌─────────────────────┐
│   EXECUTOR NODE 1   │ │   EXECUTOR NODE 2   │ │   EXECUTOR NODE N   │
│ ┌─────────────────┐ │ │ ┌─────────────────┐ │ │ ┌─────────────────┐ │
│ │ Task 1 | Task 2 │ │ │ │ Task 3 | Task 4 │ │ │ │ Task 5 | Task 6 │ │
│ └─────────────────┘ │ │ └─────────────────┘ │ │ └─────────────────┘ │
│ [ Cache / Storage ] │ │ [ Cache / Storage ] │ │ [ Cache / Storage ] │
└─────────────────────┘ └─────────────────────┘ └─────────────────────┘
```

#### 1️⃣ Driver Node (The Master / Brain)
- **Role**: Main process that runs the user's `main()` program.
- **Responsibilities**:
  - Creates the `SparkSession` / `SparkContext`.
  - Converts user code into a **DAG (Directed Acyclic Graph)** execution plan.
  - Splits work into **Stages** and **Tasks**.
  - Schedules and assigns tasks to **Executors**.
  - Collects results (when actions like `.collect()` are called).

#### 2️⃣ Cluster Manager (The Resource Allocator)
- **Role**: Allocates memory and CPU resources across the cluster.
- **Supported Managers**: YARN (Hadoop), Kubernetes, Mesos, or Spark Standalone.

#### 3️⃣ Executors (The Workers / Brawn)
- **Role**: Worker processes running on individual cluster nodes.
- **Responsibilities**:
  - Execute tasks assigned by the Driver.
  - Store cached data in memory or disk.
  - Send task status and results back to the Driver.

💡 **Real-World Analogy**:
Think of a **Restaurant**:
- **Driver** = Head Chef / Manager (Takes orders, plans recipes, assigns tasks to cooks).
- **Cluster Manager** = Facilities Manager (Provides kitchen space, stoves, and cooks).
- **Executors** = Line Cooks (Chopping vegetables, cooking dishes in parallel).

---

## 2. 🔌 SparkSession

### 💡 What is SparkSession?
Introduced in **Spark 2.0**, `SparkSession` is the **unified entry point** for all Spark functionality. 

Before Spark 2.0, developers had to manage multiple separate context objects:
- `SparkContext` → Core Spark / RDD operations
- `SQLContext` → Spark SQL & DataFrames
- `HiveContext` → Hive queries & tables
- `StreamingContext` → Spark Streaming

`SparkSession` encapsulates all of these into **one single unified interface**.

---

### 💻 Creating a SparkSession (PySpark)

```python
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("PySpark-Notes-App") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Access underlying SparkContext if needed
sc = spark.sparkContext

print(f"Spark Version: {spark.version}")
```

---

### ⚡ Common SparkSession Operations

```python
# 1. Read Data (CSV, JSON, Parquet, Delta, etc.)
df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)

# 2. Execute SQL Queries directly
df.createOrReplaceTempView("sales")
high_sales = spark.sql("SELECT customer_id, amount FROM sales WHERE amount > 1000")

# 3. Create DataFrame from Python Data
data = [("Alice", 30), ("Bob", 25)]
df_custom = spark.createDataFrame(data, ["Name", "Age"])

# 4. Gracefully Stop Session (Frees cluster resources)
spark.stop()
```

---

## 3. 📊 RDDs vs DataFrames

### 📦 1. Resilient Distributed Dataset (RDD)
- **What is it?** The foundational low-level data structure in Spark.
  - **Resilient**: Fault-tolerant (rebuilds lost data using lineage graph if a node fails).
  - **Distributed**: Data split across multiple partitions on cluster nodes.
  - **Dataset**: Collection of JVM / Python objects.
- **Characteristics**: Immutable, low-level API, strongly typed (in Scala/Java), **no query optimization**.

### 📋 2. DataFrame
- **What is it?** A distributed collection of data organized into **named columns** (like a SQL table or Pandas DataFrame).
- **Characteristics**: Structured with a schema, optimized under the hood by **Catalyst Optimizer** and **Tungsten Engine**, fast and easy to use.

---

### 🆚 Comparison Matrix

| Feature | RDD (Resilient Distributed Dataset) | DataFrame |
|---|---|---|
| **Abstraction Level** | Low-Level API | High-Level API |
| **Data Structure** | Unstructured / Raw Objects | Structured (Named columns & Schema) |
| **Optimization Engine** | ❌ None (Developer must optimize code) | ✅ Catalyst Optimizer & Tungsten Engine |
| **Performance** | Slower (High serialization overhead) | Faster (Off-heap memory & optimized execution plan) |
| **Ease of Use** | Complex (Lambda functions: `map`, `flatMap`) | Simple (SQL-like: `filter`, `select`, `groupBy`) |
| **Language Support** | Scala, Java, Python | Scala, Java, Python, R, SQL |
| **Best Used For** | Low-level unstructured data (e.g. raw logs) | Structured/tabular data, ETL pipelines, Data Analytics |

---

### 💻 Code Example Comparison

#### Task: Filter users with age > 25 and count them.

**Using RDD:**
```python
# Low-level RDD approach
rdd = sc.parallelize([("Alice", 30), ("Bob", 20), ("Charlie", 28)])
filtered_rdd = rdd.filter(lambda row: row[1] > 25)
count = filtered_rdd.count()
```

**Using DataFrame:**
```python
# High-level DataFrame approach
df = spark.createDataFrame([("Alice", 30), ("Bob", 20), ("Charlie", 28)], ["Name", "Age"])
filtered_df = df.filter(df.Age > 25)
count = filtered_df.count()
```

---

## 4. ⏳ Lazy Evaluation

### 💡 What is Lazy Evaluation?
In Spark, **Lazy Evaluation** means Spark **does not execute transformations immediately** when you write them. Instead, it records the operations in a logical execution blueprint called a **DAG (Directed Acyclic Graph)**.

Actual data processing happens **ONLY when an ACTION is called**.

---

### 🔄 Transformations vs Actions

| Category | Description | Examples | Behavior |
|---|---|---|---|
| **Transformations** | Defines a new RDD/DataFrame from an existing one. | `map()`, `filter()`, `select()`, `groupBy()`, `join()` | **Lazy** (Appended to DAG, no computation yet) |
| **Actions** | Triggers computation and returns a result to Driver or writes data to storage. | `count()`, `collect()`, `show()`, `take()`, `write.save()` | **Eager** (Triggers physical execution of DAG) |

---

### 🔀 Types of Transformations

1. **Narrow Transformations**:
   - Data in one partition maps to **only one** output partition.
   - **No network shuffle required**.
   - Examples: `map()`, `filter()`, `select()`.
2. **Wide Transformations**:
   - Data from multiple partitions is required to compute output partitions.
   - **Requires Data Shuffle** (data transferred across nodes over the network).
   - Examples: `groupBy()`, `reduceByKey()`, `join()`, `distinct()`.

```
Narrow Transformation (No Shuffle):
Node 1: [ Partition 1 ] ──▶ [ Filtered Partition 1 ]
Node 2: [ Partition 2 ] ──▶ [ Filtered Partition 2 ]

Wide Transformation (Data Shuffle Required):
Node 1: [ Partition 1 ] ──┬──▶ [ Group A ] (Node 1)
                        └──▶ [ Group B ] (Node 2)
Node 2: [ Partition 2 ] ──┘
```

---

### 🎯 Key Benefits of Lazy Evaluation

1. **Query Optimization**: Catalyst Optimizer reviews the entire DAG before executing to optimize query plans (e.g. pushing down filters, combining operations).
2. **Resource Efficiency**: Avoids running unnecessary intermediate calculations or storing intermediate results in memory.
3. **Fault Tolerance**: If an executor fails, Spark re-evaluates only the lost partition's lineage path in the DAG rather than restarting from scratch.

---

### 🛠️ Execution Flow Step-by-Step

```python
# Step 1: Read Data (Transformation - LAZY)
df = spark.read.csv("large_sales_data.csv", header=True)

# Step 2: Filter Data (Transformation - LAZY)
df_filtered = df.filter(df["country"] == "India")

# Step 3: Select Columns (Transformation - LAZY)
df_selected = df_filtered.select("customer_id", "amount")

# 🛑 Up to this point, Spark HAS NOT read the file or processed any data!

# Step 4: Display Top 5 Rows (ACTION - EAGER)
df_selected.show(5)  # 🚀 Spark executes the optimized DAG now!
```

---

## 5.  Delta Lake Format

###  What is Delta Lake?
- **Delta Lake** is an open-source storage layer created by Databricks that brings **ACID transactions** and **data reliability** to cloud data lakes (ADLS Gen2, AWS S3, Google Cloud Storage).
- It stores data in standard **Parquet format** while maintaining an ordered **Transaction Log (`_delta_log/`)**.
- **Default in Databricks**: In Azure Databricks, Delta is the default format for all table creations and file writes.

---

### Parquet vs Delta Lake Comparison

| Feature | Standard Parquet File | Delta Lake Table |
|---|---|---|
| **Storage Structure** | Collection of `.parquet` files | `.parquet` files + `_delta_log/` transaction directory |
| **ACID Transactions** | ❌ No (Partial failures leave orphan/corrupt files) | ✅ Yes (Atomic commits prevent dirty/partial reads) |
| **Mutations (UPDATE/DELETE/MERGE)** | ❌ Expensive (Requires rewriting full directories) | ✅ Native Delta DML commands (`UPDATE`, `DELETE`, `MERGE INTO`) |
| **Time Travel / Auditing** | ❌ No historical tracking | ✅ Query historical data by version or timestamp |
| **Schema Enforcement** | ❌ No validation (Bad schema can corrupt tables) | ✅ Rejects incompatible writes automatically |
| **Schema Evolution** | ❌ Requires manual table recreation | ✅ Append `.option("mergeSchema", "true")` to add new columns |

---

## 6.  ACID Transactions in Delta Lake

###  What is ACID?
Delta Lake ensures data integrity across parallel batch and streaming jobs using **ACID guarantees**:
- **Atomicity**: A write job either finishes **100% completely** or **0% fails** (no half-written corrupt files).
- **Consistency**: Data strictly follows schema definitions and constraints before and after writes.
- **Isolation**: Multi-user concurrency is managed cleanly using **Optimistic Concurrency Control**. Readers never block writers, and writers never see partial commits.
- **Durability**: Once a transaction log entry is committed to ADLS/S3, it is permanent and survives system crashes.

---

###  Real-World Use Case Scenario: E-Commerce Payment Ingestion
> **Scenario**: An e-commerce platform ingests 50,000 order payments per minute into an Azure Data Lake while business analysts query the table for live financial reporting.
> - **Without Delta Lake (Raw Parquet)**: If the ingestion job crashes at 50% completion, dirty orphan Parquet files are left behind. Analysts running queries get corrupted totals and duplicate data.
> - **With Delta Lake**: The failed batch is aborted completely without writing an entry in `_delta_log`. Analysts continue querying the last consistent state uninterrupted.

---

## 7.  Transaction Log (`_delta_log`)

###  How Delta Lake Works Under the Hood
Inside every Delta table directory, Delta maintains a `_delta_log/` subfolder containing sequential JSON commit logs and periodic Parquet checkpoints.

```
abfss://silver@secondstorage89.dfs.core.windows.net/orders/
├── _delta_log/
│   ├── 00000000000000000000.json             # Commit 0: Table Creation
│   ├── 00000000000000000001.json             # Commit 1: Batch 1 Append
│   ├── 00000000000000000002.json             # Commit 2: UPDATE / DELETE Operation
│   └── 00000000000000000010.checkpoint.parquet # Compact snapshot of log state
├── part-00000-a1b2c3d4.snappy.parquet       # Physical Parquet data file
└── part-00001-e5f6g7h8.snappy.parquet       # Physical Parquet data file
```

Every commit file records exact actions:
- **`add`**: Indicates new Parquet files added to the table snapshot.
- **`remove`**: Indicates old Parquet files marked logical deleted (tombstoned).
- **`commitInfo`**: Metadata detailing timestamp, user, notebook, and operation type (`WRITE`, `MERGE`, `DELETE`).

---

### Time Travel (Querying & Restoring Historical Versions)

Because the `_delta_log/` records every change, you can query historical snapshots of data without creating expensive data copies.

####  Code Examples (PySpark):

```python
# Option A: Query historical table state by Version Number
df_v1 = spark.read \
    .format("delta") \
    .option("versionAsOf", 1) \
    .load("abfss://silver@secondstorage89.dfs.core.windows.net/orders/")

# Option B: Query historical table state by Timestamp
df_yesterday = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2026-08-24 10:00:00") \
    .load("abfss://silver@secondstorage89.dfs.core.windows.net/orders/")

# Option C: Roll back / Restore a table to an earlier clean version
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "abfss://silver@secondstorage89.dfs.core.windows.net/orders/")
deltaTable.restoreToVersion(1)
```

---

## 8.  Schema Enforcement vs Schema Evolution

### 1️⃣ Schema Enforcement (Schema Validation)
- **Definition**: Delta Lake **prevents data corruption** by verifying that newly incoming DataFrames match the expected table schema (column names and data types).
- **Behavior**: If extra columns exist or data types clash, Delta **throws an AnalysisException** and rejects the write.

####  Code Example:
```python
# Target Silver Schema: [order_id: STRING, amount: DECIMAL(12,2)]
# Incoming DataFrame contains an unexpected extra column: [unknown_col: STRING]

try:
    df_new.write.format("delta").mode("append").save("/mnt/silver/orders")
except Exception as e:
    print(" Write Rejected by Delta Schema Enforcement!")
    # AnalysisException: A schema mismatch detected when writing to the Delta table.
```

---

### 2️⃣ Schema Evolution (Schema Merging)
- **Definition**: The process of **automatically updating the table schema** to accommodate new legitimate fields introduced upstream over time.
- **How to enable**: Pass `.option("mergeSchema", "true")` during write operations or set `spark.databricks.delta.schema.autoMerge.enabled = true`.

####  Code Example:
```python
# Evolve Delta schema safely by enabling mergeSchema
df_new.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("abfss://silver@secondstorage89.dfs.core.windows.net/orders/")

print("✅ Schema evolved successfully! New columns added without breaking existing data.")
```

---

###  Real-World Use Case Scenario: Upstream API Upgrades
> **Scenario**: An online marketplace app releases an update that adds a new `discount_code` field to incoming JSON orders.
> - **Without Schema Evolution**: The pipeline crashes instantly because Delta Schema Enforcement protects existing downstream BI dashboards from unexpected columns.
> - **With Schema Evolution (`mergeSchema=True`)**: The pipeline gracefully adds the `discount_code` column to the Silver Delta table. Historical records receive a `NULL` value for `discount_code` automatically, ensuring zero pipeline downtime.

---

## 9.  Delta Lake Time Travel (`VERSION AS OF` / `TIMESTAMP AS OF`)

### What is Time Travel?
- **Time Travel** allows you to query or restore earlier snapshots of a Delta table using either a **Version Number** or a **Timestamp**.
- **How it Works**: Delta Lake uses the `_delta_log/` commit history to reconstruct the exact set of Parquet files that were active at any specific point in history.

---

###  Code Examples (PySpark & SQL)

```sql
-- 1. SQL Query by Version Number
SELECT * FROM silver_orders VERSION AS OF 2;

-- 2. SQL Query by Timestamp
SELECT * FROM silver_orders TIMESTAMP AS OF '2026-08-24 10:00:00';
```

```python
# 1. PySpark Read by Version Number
df_v2 = spark.read \
    .format("delta") \
    .option("versionAsOf", 2) \
    .load("abfss://silver@secondstorage89.dfs.core.windows.net/orders/")

# 2. PySpark Read by Timestamp
df_time = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2026-08-24 10:00:00") \
    .load("abfss://silver@secondstorage89.dfs.core.windows.net/orders/")

# 3. Restore Table to a Previous Clean Version
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "abfss://silver@secondstorage89.dfs.core.windows.net/orders/")
deltaTable.restoreToVersion(2)
```

---

###  Real-World Scenario: Regulatory Audits & Accidental Writes
> **Scenario**: A buggy deployment executed an unconstrained UPDATE query that corrupted order amounts in production at 2:00 PM.
> - **Resolution**: Read `TIMESTAMP AS OF '2026-08-25 01:59:00'` to verify data before the incident, or execute `deltaTable.restoreToTimestamp('2026-08-25 01:59:00')` to instantly revert the entire production table without restoring from storage backups.

---

## 10.  MERGE (Upserts)

###  What is MERGE INTO?
- **`MERGE INTO`** enables atomic **Upserts** (inserting new records while updating existing records, and optionally deleting matching records) in a single, unified transaction.
- Traditional data lake formats require rewriting whole directories to update single rows. Delta Lake reads affected Parquet files, writes new updated versions, and atomically updates `_delta_log`.

---

###  Code Examples (PySpark & SQL)

```sql
-- SQL MERGE INTO (Upsert)
MERGE INTO silver_customers AS target
USING bronze_updates AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET target.email = source.email, target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, email, updated_at)
  VALUES (source.customer_id, source.name, source.email, source.updated_at);
```

```python
# PySpark Delta MERGE Example
from delta.tables import DeltaTable

silverTable = DeltaTable.forPath(spark, "abfss://silver@secondstorage89.dfs.core.windows.net/customers/")

silverTable.alias("target").merge(
    source=df_bronze_updates.alias("source"),
    condition="target.customer_id = source.customer_id"
).whenMatchedUpdate(set={
    "email": "source.email",
    "updated_at": "source.updated_at"
}).whenNotMatchedInsert(values={
    "customer_id": "source.customer_id",
    "name": "source.name",
    "email": "source.email",
    "updated_at": "source.updated_at"
}).execute()
```

---

###  Real-World Scenario: CDC (Change Data Capture) Processing
> **Scenario**: An operational database streams daily customer profile changes (inserts and updates) into Azure Data Lake.
> - **Without MERGE**: Ingesting daily batches creates duplicate customer rows or requires expensive full-table overwrites.
> - **With MERGE**: Delta matches incoming records on `customer_id`, updating existing profiles and inserting new customers in a single atomic transaction.

---

## 11.  DELETE & UPDATE Operations

###  How DELETE & UPDATE Work in Delta Lake
- Delta Lake natively supports standard `UPDATE` and `DELETE` DML commands.
- **Under the Hood (Copy-on-Write)**: Delta identifies Parquet files containing rows matching the search condition, writes new Parquet files with updated or omitted rows, and marks old files as `remove` in `_delta_log`.

---

### Code Examples (PySpark & SQL)

```sql
-- 1. SQL UPDATE
UPDATE silver_orders 
SET order_status = 'CANCELLED' 
WHERE order_delivered_customer_date IS NULL AND order_status = 'delivered';

-- 2. SQL DELETE
DELETE FROM silver_customers 
WHERE account_status = 'INACTIVE' AND last_login_date < '2023-01-01';
```

```python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "abfss://silver@secondstorage89.dfs.core.windows.net/orders/")

# 1. PySpark UPDATE
deltaTable.update(
    condition="order_delivered_customer_date IS NULL AND order_status = 'delivered'",
    set={"order_status": "'CANCELLED'"}
)

# 2. PySpark DELETE
deltaTable.delete(condition="account_status = 'INACTIVE'")
```

---

###  Real-World Scenario: GDPR / CCPA "Right to be Forgotten"
> **Scenario**: A customer submits a formal privacy request (GDPR) demanding complete deletion of their personal records.
> - **Resolution**: Run `deltaTable.delete("customer_unique_id = 'CUST_9988'")` to remove all matching personal identity records cleanly from active production table snapshots.

---

## 12.  VACUUM (Cleaning Unused Historical Files)

###  What is VACUUM?
- Over time, updates, deletes, and overwrites leave unreferenced (tombstoned) Parquet files on storage.
- **`VACUUM`** permanently deletes files no longer referenced by active Delta table state that are older than a retention threshold (**default: 7 days / 168 hours**).

---

###  Retention Safety & Constraints
- **Default Threshold**: 168 hours (7 days) to protect active Time Travel queries and long-running jobs.
- **Safety Guardrail**: Databricks blocks running `VACUUM` with retention less than 168 hours unless `spark.databricks.delta.retentionDurationCheck.enabled = false` is configured.

---

###  Code Examples (PySpark & SQL)

```sql
-- 1. SQL VACUUM (Default 7-day retention)
VACUUM silver_orders;

-- 2. SQL VACUUM with explicit retention hours
VACUUM silver_orders RETAIN 168 HOURS;
```

```python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "abfss://silver@secondstorage89.dfs.core.windows.net/orders/")

# Vacuum unreferenced files older than 168 hours (7 days)
deltaTable.vacuum(168)
```

---

###  Real-World Scenario: Storage Cost & Storage Hygiene Optimization
> **Scenario**: A streaming pipeline running for months generates millions of stale, unreferenced Parquet files, inflating cloud storage bills.
> - **Resolution**: Execute `VACUUM silver_orders RETAIN 168 HOURS` on a weekly schedule to permanently purge unneeded Parquet files and reduce storage costs.

---
## 13.  OPTIMIZE & Z-Ordering (Performance Tuning)

###  What are OPTIMIZE and Z-ORDER?
- **Small File Problem**: High-frequency streaming or small batch writes create thousands of tiny Parquet files (< 10 MB), degrading Spark query speeds due to excessive filesystem metadata scans.
- **`OPTIMIZE` (Bin-Packing / Compaction)**: Merges clusters of small Parquet files into larger, uniform files (~1 GB).
- **`Z-ORDER` (Multidimensional Clustering)**: Co-locates related data in the same set of files based on specified high-cardinality columns to maximize **Data Skipping**.

---

###  Code Examples (PySpark & SQL)

```sql
-- 1. File Compaction
OPTIMIZE silver_orders;

-- 2. Compaction + Z-Ordering on search columns
OPTIMIZE silver_orders 
ZORDER BY (customer_id, order_purchase_timestamp);
```

```python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "abfss://silver@secondstorage89.dfs.core.windows.net/orders/")

# Execute OPTIMIZE with Z-Ordering in PySpark
deltaTable.optimize().executeZOrderBy(["customer_id", "order_purchase_timestamp"])
```

---

###  Real-World Scenario: Query Acceleration on BI Dashboards
> **Scenario**: BI dashboards filtering orders by `customer_id` take 5 minutes to render because Spark scans 50,000 tiny files.
> - **Resolution**: Run `OPTIMIZE silver_orders ZORDER BY (customer_id)`. Spark compacts small files into 1 GB files and uses Z-Order statistics to skip 95% of irrelevant files, bringing query execution down from 5 minutes to 3 seconds.

---

## 14. 🧩 Partitioning Strategies

### 💡 In-Memory Partitioning vs Storage Partitioning
1. **In-Memory Partitioning**:
   - Refers to how data is split into partitions across **executor nodes in memory** for parallel CPU processing.
   - Each partition is processed by a single CPU thread.
2. **Storage / File Partitioning (`partitionBy`)**:
   - Refers to how data is physically saved to **disk or cloud storage** (ADLS/S3) in a directory hierarchy.
   - Example folder layout: `/mnt/orders/year=2026/month=08/part-0000.parquet`

---

### 🔑 Choosing the Right Partition Key & Cardinality Rules
- **Ideal Partition Keys**: Low-to-Medium cardinality columns that are frequently used in `WHERE` clauses (e.g., `year`, `month`, `country`, `department`).
- **High Cardinality Risk (e.g. `user_id`, `timestamp`, `order_id`)**:
  - Creates millions of tiny sub-folders (the **Small File Problem**).
  - Overhead of reading/writing file metadata slows down queries significantly.
- **Low Cardinality Risk (e.g. `gender`, `boolean_flag`)**:
  - Creates giant, unbalanced partitions, leading to **Data Skew** and idle executor cores.

---

### 🚀 Partition Pruning
- **Definition**: Spark automatically reads **ONLY the relevant subfolders** matching the query filter and ignores all unneeded partition directories on storage.

#### 💻 Code Example (PySpark):
```python
# 1. Write partitioned data to ADLS / Storage
df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .save("abfss://silver@secondstorage89.dfs.core.windows.net/orders/")

# 2. Partition Pruning in Action
# Spark scans ONLY the folder: .../orders/year=2026/month=08/
df_august = spark.read \
    .format("delta") \
    .load("abfss://silver@secondstorage89.dfs.core.windows.net/orders/") \
    .filter((F.col("year") == 2026) & (F.col("month") == 8))
```

---

### 💼 Real-World Scenario: Multi-TB E-Commerce Partitioning
> **Scenario**: A dataset contains 500 Million order records spanning 5 years. Queries frequently analyze monthly sales reports.
> - **Bad Strategy**: Partitioning by `customer_id` creates 10 Million folders containing 1 KB files.
> - **Optimal Strategy**: Partitioning by `year` and `month` creates ~60 well-sized folders (~5 GB each), enabling instant **Partition Pruning** during monthly queries.

---

## 15. 🔀 `repartition()` vs `coalesce()`

### 💡 Core Concept
Both methods change the number of partitions in a DataFrame, but they use fundamentally different execution mechanisms under the hood.

---

### 🆚 Comparison Matrix

| Feature | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| **Data Shuffle** | ✅ **Full Shuffle** (Data moves across network) | ❌ **No Shuffle** (Combines local adjacent partitions) |
| **Increase Partitions?** | ✅ Yes (e.g. 10 ➔ 100) | ❌ No (Cannot increase partition count) |
| **Decrease Partitions?** | ✅ Yes (e.g. 100 ➔ 10) | ✅ Yes (Efficiently decreases partitions) |
| **Data Balance** | Balances data **evenly** across all target partitions | Can result in **uneven** partition sizes |
| **Performance** | Expensive (Network I/O & CPU overhead) | Very Fast & Lightweight |
| **Primary Use Case** | Fixing **Data Skew** or increasing parallelism | Reducing output files **before saving to disk** |

---

### 💻 Code Examples (PySpark):

```python
# 1. Increasing partitions to boost parallelism (Triggers Full Shuffle)
df_boosted = df.repartition(100)

# 2. Repartitioning by a specific column to group data in memory
df_by_country = df.repartition("country")

# 3. Reducing partitions before saving to storage (No Shuffle - Fast!)
# Avoids creating 200 tiny files on disk
df_filtered.coalesce(1).write.csv("abfss://output@storage/single_file.csv")
```

---

### 💼 Real-World Scenario: File Cleanup Before Storage Export
> **Scenario**: After filtering a 100 GB DataFrame down to a small 50 MB summary report, saving directly produces 200 tiny files of ~250 KB each.
> - **With `repartition(1)`**: Triggers an unnecessary full network shuffle of all data across cluster nodes.
> - **With `coalesce(1)`**: Combines local partitions without network shuffling, producing a single clean 50 MB output file fast.

---

## 16. 🌪️ Shuffle Operations

### 💡 What is a Data Shuffle?
- **Data Shuffle** is the process of re-distributing data across executors over the cluster network so that records sharing the same key reside on the same worker node.
- **Triggered By**: **Wide Transformations** (`groupBy()`, `join()`, `distinct()`, `reduceByKey()`, `repartition()`).

```
Executor 1: [ NY, CA, TX ] ──┐         ┌──▶ Executor 1: [ NY, NY, NY ]
                             ├──SHUFFLE──┼──▶ Executor 2: [ CA, CA, CA ]
Executor 2: [ NY, CA, TX ] ──┘ (Network) └──▶ Executor 3: [ TX, TX, TX ]
```

---

### ⚠️ Performance Cost of Shuffling
Shuffling is the **most expensive operation** in Spark because it involves:
1. **Disk I/O**: Writing intermediate shuffle files to local disk.
2. **Network I/O**: Transferring data streams across cluster nodes over the network.
3. **Serialization & Deserialization**: Encoding JVM objects into bytes and back.
4. **Garbage Collection (GC)**: Memory pressure from shuffling can cause GC pauses or Out-Of-Memory (`OOM`) errors.

---

### ⚙️ `spark.sql.shuffle.partitions` Tuning
- **Default Value**: `200`
- **Tuning Rule**:
  - **Small Data (< 1 GB)**: Reduce to `4` - `16` (Default 200 causes 200 tiny task overheads).
  - **Large Data (> 100 GB)**: Increase to `1000` - `2000` (Default 200 causes huge partitions > 2 GB, throwing `OOM`).
  - **Target Size per Partition**: Aim for **100 MB – 200 MB** per partition post-shuffle.

---

### 🚀 Shuffle Optimization Techniques

1. **Broadcast Join (`broadcast()`)**:
   - If one DataFrame is small (< 10 MB default), Spark copies the small table to **all executor nodes**, converting a wide shuffle join into a **Fast Narrow Map-Side Join**.
2. **Filter Early**:
   - Apply filters and column selection (`select()`) **BEFORE** invoking wide transformations.
3. **Enable AQE (Adaptive Query Execution)**:
   - In Spark 3+, setting `spark.sql.adaptive.enabled = true` dynamically merges small post-shuffle partitions at runtime.

#### 💻 Code Example:
```python
from pyspark.sql.functions import broadcast

# Set shuffle partitions for medium dataset
spark.conf.set("spark.sql.shuffle.partitions", "20")

# Broadcast Join (No Shuffle!)
df_large_orders = spark.read.table("orders")
df_small_lookup = spark.read.table("country_lookup")

# Broadcast lookup table to bypass network shuffle completely
df_joined = df_large_orders.join(broadcast(df_small_lookup), "country_id")
```

---

## 17. 💾 Caching & Persisting (`cache()` & `persist()`)

### 💡 Why Cache Data?
Because Spark uses **Lazy Evaluation**, re-using the same DataFrame in multiple downstream actions will cause Spark to **re-evaluate the entire upstream DAG from scratch** every time.
Caching stores computed DataFrames in memory/disk so subsequent actions read instantly from memory.

---

### 🆚 `cache()` vs `persist()`

- **`.cache()`**: Shorthand for `.persist(StorageLevel.MEMORY_AND_DISK)` in DataFrames (stores in RAM; spills to disk if memory is full).
- **`.persist(storage_level)`**: Allows explicit choice of storage level based on memory vs CPU tradeoffs.

---

### 📦 Storage Levels Breakdown

| Storage Level | Memory Usage | Disk Usage | CPU Overhead | Recomputation |
|---|---|---|---|---|
| `MEMORY_ONLY` | High | None | Low | Recomputes lost partitions from DAG |
| `MEMORY_AND_DISK` | Medium | Low (Spills to disk if RAM full) | Medium | Reads spilled blocks from local disk |
| `DISK_ONLY` | Low | High | High | Reads all data from local disk |
| `MEMORY_ONLY_SER` | Low (Serialized bytes) | None | High (Requires CPU deserialization) | Recomputes lost partitions |
| `*_2` (e.g. `MEMORY_ONLY_2`) | 2x Memory | None | Low | Replicated on 2 nodes for fault-tolerance |

---

### 🧹 Releasing Cache (`unpersist()`)
Cached data remains in memory until evicted or until the SparkSession stops. Always call `.unpersist()` when the cached dataset is no longer needed to prevent memory exhaustion.

---

### 💻 Code Example (PySpark):

```python
from pyspark.storagelevel import StorageLevel

# Read & Filter Bronze layer
df_clean = spark.read.parquet("/mnt/bronze/sales").filter("amount > 0")

# Cache in memory & spill to disk if memory full
df_clean.persist(StorageLevel.MEMORY_AND_DISK)

# Action 1: First trigger (Computes DAG and saves to cache)
total_count = df_clean.count()

# Action 2: Second trigger (Reads instantly from Cache - Fast!)
summary_df = df_clean.groupBy("category").sum("amount")
summary_df.show()

# 🧹 Clean up memory!
df_clean.unpersist()
```

---

---

## 18. 📡 Broadcast Joins (Map-Side Joins)

### 💡 How Broadcast Join Works
- **Mechanism**: Instead of triggering a full network data shuffle across the cluster, Spark copies (broadcasts) the **entire small DataFrame** to the memory of **every executor node**.
- **Execution**: Each executor performs a fast **local hash join** (Map-Side Join) between its partition of the large DataFrame and the cached small lookup DataFrame.

```
Driver Node (Broadcasts small_df) ───┬──▶ Executor 1: [ large_p1 JOIN small_df ]
                                     ├──▶ Executor 2: [ large_p2 JOIN small_df ]
                                     └──▶ Executor 3: [ large_p3 JOIN small_df ]
                                     (No Large Data Shuffle Over Network!)
```

---

### ⚙️ Configuration & Hints
- **Auto-Broadcast Threshold**: `spark.sql.autoBroadcastJoinThreshold`
  - Default: `10 MB` (`10485760` bytes).
  - Set to `-1` to disable auto-broadcasting.
- **Explicit Broadcast Hint**: Override automatic thresholds using `broadcast()` in PySpark or `/*+ BROADCAST(small_table) */` in SQL.

---

### 💻 Code Examples (PySpark & SQL):

```python
from pyspark.sql.functions import broadcast

# 1. PySpark Explicit Broadcast Join
df_fact_sales = spark.read.table("fact_sales")
df_dim_store = spark.read.table("dim_store")  # Small table (~5 MB)

df_joined = df_fact_sales.join(
    broadcast(df_dim_store),
    df_fact_sales.store_id == df_dim_store.store_id,
    "inner"
)
```

```sql
-- 2. Spark SQL Broadcast Hint
SELECT /*+ BROADCAST(dim_store) */ 
    s.sale_id, s.amount, d.store_name
FROM fact_sales s
JOIN dim_store d ON s.store_id = d.store_id;
```

---

### ⚠️ Limitations & Out-Of-Memory (OOM) Risks
- **OOM on Driver/Executor**: If the broadcast dataset is too large (e.g. > 1 GB), transmitting it causes `java.lang.OutOfMemoryError: Java heap space` on the Driver or Executor nodes.
- **Rule of Thumb**: Only broadcast dimension / lookup tables that fit comfortably in memory (< 100 MB).

---

## 19. 🗜️ Z-Ordering & File Compaction

### 💡 The Small File Problem
High-frequency batch writes or streaming jobs produce thousands of tiny Parquet files (< 10 MB). Reading thousands of tiny files causes severe metadata scanning overhead and slows down queries.

---

### 📦 1. File Compaction (`OPTIMIZE`)
- **Bin-Packing Algorithm**: Merges clusters of small Parquet files into larger, uniform files (typically target size **1 GB**).
- **Databricks Auto-Compaction & Optimized Writes**:
  - `spark.databricks.delta.autoCompact.enabled = true` (Automatically compacts after writes).
  - `spark.databricks.delta.optimizeWrite.enabled = true` (Dynamically resizes files during writes).

---

### 🎯 2. Z-Ordering (`ZORDER BY`)
- **Multidimensional Clustering**: Co-locates related data within the same Parquet files along specified high-cardinality columns.
- **Data Skipping**: Delta Lake stores min/max statistics for every column per file in `_delta_log`. Z-Ordering aligns data ranges so Spark can **skip 90%+ of unneeded Parquet files** during read operations.

---

### 🆚 File Partitioning vs Z-Ordering

| Feature | File Partitioning (`partitionBy`) | Z-Ordering (`ZORDER BY`) |
|---|---|---|
| **Structure** | Creates physical sub-directories on disk | Organizes data **inside** Parquet files |
| **Ideal Cardinality** | Low-to-Medium (e.g. `year`, `month`, `state`) | High Cardinality (e.g. `customer_id`, `timestamp`) |
| **Multi-Column Support**| Bad (Causes folder explosion) | Excellent (Clusters across multiple columns) |
| **Maintenance** | Applied at write time | Applied on-demand via `OPTIMIZE` command |

---

### 💻 Code Examples (PySpark & SQL):

```sql
-- Compact small files AND apply Z-Ordering on customer_id & order_date
OPTIMIZE silver_orders 
ZORDER BY (customer_id, order_date);
```

```python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "abfss://silver@secondstorage89.dfs.core.windows.net/orders/")

# Execute Compaction and Z-Ordering in PySpark
deltaTable.optimize().executeZOrderBy(["customer_id", "order_date"])
```

---

## 20. 🤖 Adaptive Query Execution (AQE)

### 💡 What is AQE?
Introduced in **Spark 3.0+** (enabled by default: `spark.sql.adaptive.enabled = true`), **Adaptive Query Execution (AQE)** re-evaluates and optimizes query execution plans **at runtime** using actual runtime statistics collected from completed shuffle stages.

---

### 🌟 3 Key Capabilities of AQE

```
                          ┌───────────────────────────┐
                          │ ADAPTIVE QUERY EXECUTION  │
                          └─────────────┬─────────────┘
                                        │
           ┌────────────────────────────┼────────────────────────────┐
           ▼                            ▼                            ▼
1. Dynamic Partition Coalescing   2. Dynamic Join Strategy    3. Dynamic Skew Join
  Merges tiny post-shuffle           Converts SMJ to            Splits skewed partitions
  partitions automatically.          Broadcast Join.            & prevents stragglers.
```

#### 1️⃣ Dynamic Coalescing of Post-Shuffle Partitions
- **Problem**: Setting `spark.sql.shuffle.partitions = 200` creates 200 tiny partitions if output data is small.
- **AQE Solution**: At runtime, AQE automatically merges adjacent small post-shuffle partitions into larger, optimal partitions (~128 MB).
- **Config**: `spark.sql.adaptive.coalescePartitions.enabled = true`

#### 2️⃣ Dynamic Switching Join Strategies
- **Problem**: Catalyst Optimizer initially plans a heavy **Sort Merge Join (SMJ)** because estimated table sizes were large.
- **AQE Solution**: If filtering in early stages reduces one table size below `autoBroadcastJoinThreshold` (e.g. 8 MB), AQE dynamically converts the SMJ to a fast **Broadcast Hash Join (BHJ)** at runtime!

#### 3️⃣ Dynamic Skew Join Handling
- **Problem**: A skewed key causes 1 task to process 90% of data while 199 tasks finish in seconds.
- **AQE Solution**: AQE detects skewed partitions at runtime, splits the skewed partition into smaller sub-partitions, and duplicates corresponding rows on the other join side.
- **Config**: `spark.sql.adaptive.skewJoin.enabled = true`

---

## 21. ⚖️ Data Skew & Handling Techniques

### 💡 What is Data Skew?
- **Data Skew** occurs when data is distributed **unevenly** across cluster partitions.
- **Symptom in Spark UI**: 99 tasks finish in 5 seconds, but **1 last task remains stuck at 99%** for 40 minutes (a "straggler task") or crashes with an Out-Of-Memory (`OOM`) error.
- **Common Causes**: Null values, default dummy keys (e.g., `customer_id = -1`), or dominant hot keys (e.g., `seller_id = 'AMAZON'`).

---

### 🛠️ Data Skew Resolution Strategies

#### Strategy 1: Salting Key Technique (For Skewed Joins)
- **Concept**: Append a random integer ("salt") between `0` and `N-1` to the skewed join key in the main dataset, and explode the lookup dataset key by `0..N-1` so keys match evenly across worker nodes.

#### 💻 Salting Code Example (PySpark):

```python
from pyspark.sql import functions as F

# 1. Add random salt (0 to 3) to the skewed dataset
SALT_NUM = 4
df_skewed_salted = df_orders.withColumn(
    "salted_customer_id",
    F.concat(F.col("customer_id"), F.lit("_"), F.floor(F.rand() * SALT_NUM))
)

# 2. Explode the lookup table keys across 0..SALT_NUM-1
df_lookup_exploded = df_customers.withColumn(
    "salt_array", F.array([F.lit(i) for i in range(SALT_NUM)])
).withColumn(
    "salt", F.explode(F.col("salt_array"))
).withColumn(
    "salted_customer_id",
    F.concat(F.col("customer_id"), F.lit("_"), F.col("salt"))
)

# 3. Join on the salted key (Spreads hot keys across 4 partitions evenly!)
df_joined = df_skewed_salted.join(
    df_lookup_exploded,
    "salted_customer_id",
    "inner"
).drop("salted_customer_id", "salt_array", "salt")
```

---

#### Strategy 2: Filter / Isolate Hot Keys
- Filter out `NULL` or dummy values (`customer_id != -1`) before joining, process valid keys, and handle nulls separately.

#### Strategy 3: Broadcast Join
- If the join partner table is small, broadcasting it completely bypasses network partitioning and eliminates key skew stragglers.

---

## 🎯 Quick Interview Cheat Sheet

- **Q: What is a Broadcast Join and when should you use it?**
  - *A: A join where Spark copies the small dataset (< 10 MB default) to all executor memories to perform a local map-side join, avoiding network data shuffling. Use it when joining a large fact table with a small dimension table.*
- **Q: What is the difference between Partitioning and Z-Ordering?**
  - *A: Partitioning creates physical subdirectories on disk for low-cardinality columns (`year`, `month`). Z-Ordering clusters data inside Parquet files for high-cardinality columns (`customer_id`), maximizing min/max Data Skipping.*
- **Q: What problem does `OPTIMIZE` solve in Delta Lake?**
  - *A: It solves the **Small File Problem** by compacting clusters of tiny Parquet files into uniform ~1 GB files.*
- **Q: What are the 3 main features of Adaptive Query Execution (AQE)?**
  - *A: 1) Dynamic post-shuffle partition coalescing, 2) Dynamic join strategy switching (SMJ to BHJ), and 3) Dynamic skew join optimization.*
- **Q: How does the Salting technique resolve Data Skew?**
  - *A: Salting appends a random prefix/suffix (`0..N-1`) to the skewed join key in the primary table and explodes lookup keys accordingly, spreading hot key rows evenly across multiple CPU partitions.*
