# 🚀 Mini-Project 2: Medallion Architecture Pipeline & Transformation Output Benchmarks

## 📌 Project Overview
This project delivers an end-to-end **Data Engineering Medallion Architecture Pipeline (Bronze ➔ Silver ➔ Gold)** built with **PySpark** and **Delta Lake** on **Databricks**.

Unlike generic scripts, **every notebook produces explicit BEFORE vs AFTER comparative metrics and output proofs** explaining **WHY** each transformation is used instead of blindly applying it.

---

## 🏗️ Architecture & Output Proof Flow

```
 ┌────────────────────────┐
 │   01_Generate_Raw_CSV  │ Generates 270k+ synthetic records with dirty data
 └───────────┬────────────┘
             │ CSV Landing (/tmp/mini_project2/landing/)
             ▼
 ┌────────────────────────┐
 │ 02_Ingestion_to_Bronze │ [PROOF 1] Explicit Schema (3x faster than inferSchema)
 └───────────┬────────────┘ [PROOF 2] Audit Lineage Columns (_ingested_at, _source_file)
             │ Bronze Delta (/tmp/mini_project2/bronze/)
             ▼
 ┌────────────────────────┐
 │03_Silver_Transformations│ [PROOF 1] Window Deduplication (Prevents revenue inflation)
 └───────────┬────────────┘ [PROOF 2] Quarantine Pattern (Saves corrupted records vs dropna)
             │              [PROOF 3] repartition() vs. coalesce() (Shuffle I/O vs Local Merge)
             │ Silver Delta (/tmp/mini_project2/silver/)
             ▼
 ┌────────────────────────┐
 │04_Gold_Optimization_ZOrder│ [PROOF 1] Gold Business Aggregations (Star Schema)
 └────────────────────────┘ [PROOF 2] OPTIMIZE Compaction (26 files -> 1 file)
                            [PROOF 3] Z-ORDER BY Data Skipping (Skips 90%+ file scans)
```

---

## 📂 Repository Structure

```
Mini-project2/
├── 01_Generate_Raw_CSV_Files.ipynb           # Standalone Synthetic Raw CSV Data Generator
├── 02_Ingestion_to_Bronze.ipynb              # Ingests Raw CSVs -> Bronze Delta Tables with lineage audit & schema proofs
├── 03_Silver_Transformations_Partitioning.ipynb # Silver cleaning, deduplication, quarantine & repartition vs coalesce proofs
├── 04_Gold_Optimization_ZOrder.ipynb         # Gold Customer 360, Daily Sales, OPTIMIZE & Z-ORDER BY comparative benchmarks
└── README.md                                  # Complete Project Guide & Comparative Transformation Proofs
```

---

## 🔬 "Why We Use This Transformation" — Comparative Output Demonstrations

### 1. Ingestion Stage: Explicit Schema vs. `inferSchema=True`
* **Output Evidence**:
  * `inferSchema=True`: Scans the entire raw CSV file twice to guess column types. Slow and risks schema drift.
  * `schema(explicit_struct)`: Scans the CSV file once. 3x-5x faster and production-safe.
* **Printed Verdict**: *"inferSchema scans the entire file TWICE to guess types, slowing down ingestion and risking unexpected schema shifts when columns change!"*

---

### 2. Silver Stage: Deduplication & Quarantine Pattern
* **Output Evidence**:
  * **Window Deduplication**: Shows **2,000 duplicate rows identified and removed** using `Window.partitionBy("order_id")`. Proves why deduplication prevents revenue inflation.
  * **Quarantine Pattern**: Displays **Valid Rows Saved** in `silver_orders` vs **Corrupted Rows Isolated** in `silver_quarantine_orders`.
* **Printed Verdict**: *"Silently dropping bad rows (`dropna()`) leads to unexplainable data loss during financial audits. Quarantining routes corrupted records to an isolated table for data ops investigation!"*

---

### 3. Silver Partitioning: `repartition()` vs. `coalesce()`
* **Output Evidence**:
  * **`repartition(10, 'region')`**: Shows **10 Partitions**, **10 Output Files**, and the `Exchange hashpartitioning` operator in the physical plan (Network Shuffle Cost).
  * **`coalesce(1)`**: Shows **1 Partition**, **1 Output File**, and the **ABSENCE of `Exchange` operator** in the physical plan (0 Shuffle Cost).
* **Printed Verdict**:
  * Use `repartition()` when data is skewed or before a heavy join/aggregation to increase parallelism.
  * Use `coalesce()` after a heavy filter to collapse file counts without paying an expensive network shuffle penalty.

---

### 4. Gold Optimization: `OPTIMIZE` (Compaction)
* **Output Evidence**:
  * **BEFORE `OPTIMIZE`**: `numFiles` = 26 files, `avgFileSize` = 12 KB (Small File Fragmentation).
  * **AFTER `OPTIMIZE`**: `numFiles` = 1 file, `avgFileSize` = 300 KB (Single Compacted Parquet File).
* **Printed Verdict**: *"OPTIMIZE merges 25+ tiny fragmented files into 1 optimal Parquet file, reducing filesystem I/O operations by ~96% and dramatically accelerating query read speeds!"*

---

### 5. Gold Indexing: `Z-ORDER BY` (Multi-Dimensional Clustering & Data Skipping)
* **Output Evidence**:
  * **BEFORE Z-Order**: Query `WHERE customer_id = 'CUST_001234'` scans every single file in the table.
  * **AFTER `OPTIMIZE ... ZORDER BY (customer_id, region)`**: Query re-runs, Delta reads min/max file statistics from `_delta_log`, and skips non-matching Parquet files.
* **Printed Verdict**: *"Z-Ordering co-locates rows by customer_id within Parquet files. When filtering by customer_id, Delta reads min/max file statistics in _delta_log and skips scanning non-matching Parquet files!"*

---

## 🛠️ How to Import and Run in Databricks

1. **Commit and Push to GitHub**:
   ```bash
   git add HandsOn-Practice/Mini-project2/
   git commit -m "Add Mini-project2 Medallion Pipeline and Transformation Proofs"
   git push origin main
   ```
2. **Open Azure Databricks**:
   - Go to **Workspace** ➔ **Repos** ➔ **Add Repo**.
   - Link your GitHub repository.
3. **Execute Notebooks in Sequence**:
   1. Run `01_Generate_Raw_CSV_Files.ipynb` to create landing CSV files.
   2. Run `02_Ingestion_to_Bronze.ipynb` to view explicit schema & lineage audit proofs.
   3. Run `03_Silver_Transformations_Partitioning.ipynb` to view deduplication, quarantine, and `repartition` vs `coalesce` execution plans.
   4. Run `04_Gold_Optimization_ZOrder.ipynb` to view `OPTIMIZE` file compaction and `Z-ORDER BY` speedups.
