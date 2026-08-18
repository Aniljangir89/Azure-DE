#  Apache Spark Core — Simplified Guide

> **Apache Spark** is an open-source, distributed, in-memory data processing engine designed for fast and large-scale data analytics.

---

##  Quick Summary

| Topic | Key Concept | Simple Summary |
|---|---|---|
| **Spark Architecture** | Driver & Executors | Driver plans & orchestrates; Executors run tasks on data partitions. |
| **SparkSession** | Unified Entry Point | Single gateway to access all Spark features (DataFrames, SQL, Streaming). |
| **RDDs vs DataFrames** | Low-Level vs High-Level | RDDs = raw objects without optimization; DataFrames = structured tables with Catalyst optimization. |
| **Lazy Evaluation** | Deferred Execution | Spark builds an execution plan (DAG) and computes ONLY when an **Action** is triggered. |

---

## 1.  Spark Architecture (Driver & Executors)

Spark uses a **Master-Worker (Master-Slave)** architecture to process data across a cluster.

###  Component Overview

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

#### 1️ Driver Node (The Master / Brain)
- **Role**: Main process that runs the user's `main()` program.
- **Responsibilities**:
  - Creates the `SparkSession` / `SparkContext`.
  - Converts user code into a **DAG (Directed Acyclic Graph)** execution plan.
  - Splits work into **Stages** and **Tasks**.
  - Schedules and assigns tasks to **Executors**.
  - Collects results (when actions like `.collect()` are called).

#### 2️ Cluster Manager (The Resource Allocator)
- **Role**: Allocates memory and CPU resources across the cluster.
- **Supported Managers**: YARN (Hadoop), Kubernetes, Mesos, or Spark Standalone.

#### 3️ Executors (The Workers / Brawn)
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

## 2.  SparkSession

###  What is SparkSession?
Introduced in **Spark 2.0**, `SparkSession` is the **unified entry point** for all Spark functionality. 

Before Spark 2.0, developers had to manage multiple separate context objects:
- `SparkContext` → Core Spark / RDD operations
- `SQLContext` → Spark SQL & DataFrames
- `HiveContext` → Hive queries & tables
- `StreamingContext` → Spark Streaming

`SparkSession` encapsulates all of these into **one single unified interface**.

---

###  Creating a SparkSession (PySpark)

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

###  Common SparkSession Operations

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

## 3.  RDDs vs DataFrames

###  1. Resilient Distributed Dataset (RDD)
- **What is it?** The foundational low-level data structure in Spark.
  - **Resilient**: Fault-tolerant (rebuilds lost data using lineage graph if a node fails).
  - **Distributed**: Data split across multiple partitions on cluster nodes.
  - **Dataset**: Collection of JVM / Python objects.
- **Characteristics**: Immutable, low-level API, strongly typed (in Scala/Java), **no query optimization**.

###  2. DataFrame
- **What is it?** A distributed collection of data organized into **named columns** (like a SQL table or Pandas DataFrame).
- **Characteristics**: Structured with a schema, optimized under the hood by **Catalyst Optimizer** and **Tungsten Engine**, fast and easy to use.

---

###  Comparison Matrix

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

### Code Example Comparison

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

## 4.  Lazy Evaluation

###  What is Lazy Evaluation?
In Spark, **Lazy Evaluation** means Spark **does not execute transformations immediately** when you write them. Instead, it records the operations in a logical execution blueprint called a **DAG (Directed Acyclic Graph)**.

Actual data processing happens **ONLY when an ACTION is called**.

---

###  Transformations vs Actions

| Category | Description | Examples | Behavior |
|---|---|---|---|
| **Transformations** | Defines a new RDD/DataFrame from an existing one. | `map()`, `filter()`, `select()`, `groupBy()`, `join()` | **Lazy** (Appended to DAG, no computation yet) |
| **Actions** | Triggers computation and returns a result to Driver or writes data to storage. | `count()`, `collect()`, `show()`, `take()`, `write.save()` | **Eager** (Triggers physical execution of DAG) |

---

###  Types of Transformations

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

###  Key Benefits of Lazy Evaluation

1. **Query Optimization**: Catalyst Optimizer reviews the entire DAG before executing to optimize query plans (e.g. pushing down filters, combining operations).
2. **Resource Efficiency**: Avoids running unnecessary intermediate calculations or storing intermediate results in memory.
3. **Fault Tolerance**: If an executor fails, Spark re-evaluates only the lost partition's lineage path in the DAG rather than restarting from scratch.

---

###  Execution Flow Step-by-Step

```python
# Step 1: Read Data (Transformation - LAZY)
df = spark.read.csv("large_sales_data.csv", header=True)

# Step 2: Filter Data (Transformation - LAZY)
df_filtered = df.filter(df["country"] == "India")

# Step 3: Select Columns (Transformation - LAZY)
df_selected = df_filtered.select("customer_id", "amount")

#  Up to this point, Spark HAS NOT read the file or processed any data!

# Step 4: Display Top 5 Rows (ACTION - EAGER)
df_selected.show(5)  #  Spark executes the optimized DAG now!
```

---

##  Quick Interview Cheat Sheet

- **Q: What is the single entry point in PySpark / Spark 2.0+?**
  - *A: `SparkSession`.*
- **Q: What happens when you execute 5 transformations without an action?**
  - *A: Nothing is computed. Spark only updates the DAG execution plan.*
- **Q: Why are DataFrames preferred over RDDs?**
  - *A: DataFrames leverage the Catalyst Optimizer and Tungsten Engine for schema awareness, lower memory consumption, and query planning.*
- **Q: What is the difference between Narrow and Wide transformations?**
  - *A: Narrow transformations do not require shuffling data across nodes (e.g., `filter`), while Wide transformations require a network data shuffle (e.g., `groupBy`).*
