# 📄 Delta Lake & Performance Tuning: Decision Guide

## 📌 Topics Covered
1. **`MERGE` vs. Full Overwrite**
2. **Z-Ordering vs. Partitioning**

---

## 1. 🔄 `MERGE` vs. Full Overwrite

### 💡 Core Overview
- **`MERGE INTO` (Upsert)**: Conditionally updates existing records, inserts new records, and optionally deletes records in a single atomic Delta transaction based on a join condition.
- **Full Overwrite (`mode("overwrite")`)**: Completely replaces the target table (or a specific partition via `replaceWhere`) with the new dataset, dropping unreferenced old files.

---

### 🆚 Decision Matrix

| Metric / Requirement | Use `MERGE INTO` | Use Full Overwrite (`mode("overwrite")`) |
|---|---|---|
| **Data Update Type** | Incremental / CDC (Change Data Capture) | Full Refresh / Snapshot Re-ingestion |
| **Volume Changed** | Small to Medium percentage of dataset (< 20%) | Large percentage or entire dataset (100%) |
| **Table Size** | Multi-GB / TB Fact tables | Small lookup / Dimension tables |
| **History & Time Travel** | Retains full row-level change history in Delta Log | Overwrites table version snapshot entirely |
| **Performance Overhead** | Scans & writes *only* affected Parquet files | Rewrites the *entire* table or target partition |

---

### 🛠️ Practical Examples

#### Scenario A: Change Data Capture (CDC) ➔ **Use `MERGE INTO`**
> **Use Case**: An operational database streams daily customer updates (inserts & updates). We only want to update changed records and insert new ones into our Silver table without rewriting millions of unchanged rows.

```python
from delta.tables import DeltaTable

# Target Silver Delta Table
silver_table = DeltaTable.forPath(spark, "abfss://silver@storage/customers")

# Incremental CDC Batch
df_daily_updates = spark.read.parquet("/mnt/bronze/daily_cdc_customers")

# Atomic MERGE (Upsert)
silver_table.alias("target").merge(
    source=df_daily_updates.alias("source"),
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

#### Scenario B: Daily Dimension Lookup Refresh ➔ **Use Full Overwrite**
> **Use Case**: Every night, an external system provides a small 50 MB product category mapping table (`dim_category`). Overwriting the entire table is faster and cleaner than running a join-based MERGE.

```python
# Read incoming fresh dimension snapshot
df_categories = spark.read.json("/mnt/bronze/categories_raw.json")

# Overwrite Silver dimension table completely
df_categories.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://silver@storage/dim_category")
```

> **Pro-Tip (Selective Overwrite with `replaceWhere`)**: If overwriting only a specific partition (e.g. `month = '2026-08'`), use `replaceWhere`:
> ```python
> df_august.write \
>     .format("delta") \
>     .mode("overwrite") \
>     .option("replaceWhere", "month = '2026-08'") \
>     .save("abfss://silver@storage/orders")
> ```

---

## 2. 🗜️ Z-Ordering vs. Partitioning

### 💡 Core Overview
- **Partitioning (`partitionBy`)**: Physically separates data into sub-directories on storage based on column values (e.g. `/orders/year=2026/month=08/`).
- **Z-Ordering (`ZORDER BY`)**: Clusters data **inside Parquet files** along specified high-cardinality columns, storing min/max statistics in `_delta_log` to maximize **Data Skipping**.

---

### 🆚 Decision Matrix

| Feature | Partitioning (`partitionBy`) | Z-Ordering (`ZORDER BY`) |
|---|---|---|
| **Storage Structure** | Physical directory hierarchy on storage disk | Data layout co-location *within* Parquet files |
| **Ideal Cardinality** | **Low to Medium** (e.g. `year`, `month`, `country`, `status`) | **High Cardinality** (e.g. `customer_id`, `timestamp`, `order_id`) |
| **Number of Columns** | 1 to 2 columns maximum | 1 to 4 columns |
| **Pruning Mechanism** | **Partition Pruning** (skips reading sub-folders) | **Data Skipping** (bypasses unneeded Parquet files via min/max log stats) |
| **Risk of Misuse** | **Small File Problem** if used on high cardinality | Degrades performance if Z-Ordering on too many (>4) columns |
| **Execution Point** | Specified during write (`df.write.partitionBy()`) | Applied on-demand (`OPTIMIZE table ZORDER BY ()`) |

---

### 🛠️ Practical Examples

#### Scenario A: Partitioning by Date Parts ➔ **Use `partitionBy()`**
> **Use Case**: Queries almost always contain `WHERE year = 2026 AND month = 08`. There are only 60 distinct year/month combinations over 5 years.

```python
# Partitioning by low-cardinality columns: year and month
df_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .save("abfss://silver@storage/orders")

# Query executes Partition Pruning (reads ONLY .../year=2026/month=08/ subfolder)
df_august = spark.read \
    .format("delta") \
    .load("abfss://silver@storage/orders") \
    .filter("year = 2026 AND month = 8")
```

---

#### Scenario B: High-Cardinality Searching ➔ **Use `ZORDER BY`**
> **Use Case**: Dashboard queries filter by unique `customer_id` or `order_id`. Partitioning by `customer_id` would create 10 Million tiny folders. Instead, we Z-Order by `customer_id`.

```sql
-- Step 1: Run OPTIMIZE with Z-Ordering on customer_id
OPTIMIZE silver_orders 
ZORDER BY (customer_id);

-- Step 2: Query executes Data Skipping
-- Delta reads min/max stats in _delta_log and bypasses 95% of Parquet files!
SELECT * FROM silver_orders WHERE customer_id = 'CUST_99881';
```

---

## 🎯 Summary Rules of Thumb

1. **`MERGE` vs Overwrite**:
   - **`MERGE`**: For CDC / Incremental updates where < 20% of data changes.
   - **Overwrite**: For full table refreshes, small lookup dimensions, or partition replacement via `replaceWhere`.
2. **Partitioning vs Z-Order**:
   - **Partitioning**: For **low/medium cardinality** columns (`year`, `month`, `region`) to prune folders.
   - **Z-Ordering**: For **high cardinality** columns (`customer_id`, `timestamp`) to compact small files and prune file reads via Data Skipping.
