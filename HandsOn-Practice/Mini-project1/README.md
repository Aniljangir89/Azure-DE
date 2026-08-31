# Olist E-Commerce Data Engineering Pipeline (Databricks & Medallion Architecture)

##  Project Overview
This project implements an end-to-end Data Engineering pipeline for the **Olist Brazilian E-Commerce Dataset** using **Azure Databricks**, **PySpark**, and **Delta Lake**. The architecture follows the **Medallion Architecture pattern**, ingesting raw CSV data into a **Bronze Layer** (raw delta storage with metadata) and transforming it into a **Silver Layer** (cleaned, typed, schema-validated, and quarantined data ready for downstream analytics).

---

##  Architecture & Storage Setup

```
   ┌──────────────────────┐
   │ Azure Key Vault      │  (azure-kv-scope -> storage-account-key)
   └──────────┬───────────┘
              │ Secret Injection
              ▼
   ┌──────────────────────┐
   │ utils/storage_config │  Configures PySpark ADLS Gen2 Auth
   └──────────────────────┘
              │
   ┌──────────┴───────────┬──────────────────────────┬──────────────────────────┐
   ▼                      ▼                          ▼                          ▼
 Raw Layer              Bronze Layer               Silver Layer               Quarantine / Anomaly
(ADLS Gen2 CSVs)   (Delta + Audit Meta)     (Cleaned, Typed Delta)      (Flagged & Filtered Data)
abfss://raw-data@  abfss://bronze@          abfss://silver@             (In-Memory / Path Filter)
```

### Storage Configuration & Security
- **Storage Account**: `secondstorage89.dfs.core.windows.net`
- **Authentication**: Key Vault Secret Scope (`azure-kv-scope` -> `storage-account-key`) dynamically fetched via `dbutils.secrets` and configured in PySpark session parameters (`fs.azure.account.key...`).
- **Utility Module**: `utils/storage_config.py` centralizes secret retrieval and Spark storage authentication across all notebooks.

---

## Repository Structure

```
Mini-project1/
├── utils/
│   └── storage_config.py      # Secret scope & PySpark ADLS Gen2 config utility
├── data-ingestion.ipynb       # Exploratory ingestion & secret verification notebook
├── Ingestion/                 # Bronze Layer: Ingestion from Raw CSV to Bronze Delta
│   ├── customers.ipynb        # Ingests customer data -> abfss://bronze@.../customers/
│   ├── geolocations.ipynb     # Ingests geolocation data -> abfss://bronze@.../geolocations/
│   ├── order_items.ipynb      # Ingests order items data -> abfss://bronze@.../order-items/
│   ├── orders.ipynb           # Ingests order headers -> abfss://bronze@.../orders/
│   ├── payments.ipynb         # Ingests payment details -> abfss://bronze@.../payments/
│   ├── products.ipynb         # Ingests product catalog -> abfss://bronze@.../products/
│   ├── reviews.ipynb          # Ingests product reviews -> abfss://bronze@.../reviews/
│   └── sellers.ipynb          # Ingests seller profiles -> abfss://bronze@.../sellers/
└── transformations/           # Silver Layer: Data Cleaning, Quality Checks & Typing
    ├── customers.ipynb        # Silver customers transformation & zip code formatting
    ├── orders.ipynb           # Silver orders processing, anomaly detection & quarantine
    ├── order_items.ipynb      # Silver order items pricing & freight decimal casting
    ├── payments.ipynb         # Silver payment verification & invalid payment quarantine
    ├── products.ipynb         # Silver product catalog cleaning & null product quarantine
    └── sellers.ipynb          # Silver seller transformation setup
```

---

##  End-to-End Pipeline Workflow

### 1. Ingestion (Raw ➔ Bronze Layer)
In the Ingestion phase, raw CSV files are read from the `raw-data` ADLS Gen2 container with header preservation and schema inference.

- **Audit Columns Appended**:
  - `_ingested_at`: High-precision timestamp (`F.current_timestamp()`) recording when Spark processed the file.
  - `_source_file`: Metadata column (`F.col("_metadata.file_path")`) capturing the source file path for data lineage.
- **Output Format**: Written in **Delta format** (`.format("delta").mode("overwrite")`) into the `bronze` container.

### 2. Transformations & Quality Control (Bronze ➔ Silver Layer)
The transformation layer applies business logic rules, data type conversions, schema enforcement, and quarantining of corrupted/anomalous records:

| Entity | Bronze Input Path | Silver Output Path | Key Transformations & Quality Rules applied |
| :--- | :--- | :--- | :--- |
| **Customers** | `bronze/customers/` | `silver/customers/` | • Analyzed `customer_id` (per-order context) vs `customer_unique_id` (stable physical customer ID).<br>• Cast `customer_zip_code_prefix` from integer to `string` to preserve leading zeroes.<br>• Checked null distributions and state/city distinct counts. |
| **Orders** | `bronze/orders/` | `silver/orders/` | • Validated timestamp sequence (`purchase` ➔ `approved` ➔ `carrier` ➔ `delivered`).<br>• **Quarantine Rule**: Filtered delivered orders missing customer delivery date (`delivered_missing_date`) into quarantine.<br>• **Anomaly Flagging**: Added boolean column `_carrier_before_purchase` to flag timestamp anomalies where carrier dates preceded purchase timestamps. |
| **Order Items** | `bronze/order-items/` | `silver/order-items/` | • Validated primary key composite cardinality `(order_id, order_item_id)`.<br>• Cast financial columns `price` and `freight_value` to fixed precision `decimal(12,2)`. |
| **Payments** | `bronze/payments/` | `silver/payments/` | • Exploded/verified `payment_sequential` sequence numbers.<br>• **Quarantine Rule**: Filtered out invalid payments where `payment_type == 'not_defined'` and `payment_value == 0`. |
| **Products** | `bronze/products/` | `silver/products/` | • Evaluated null occurrences across category, dimensions, and specifications.<br>• **Quarantine Rule**: Filtered out completely empty product records (all metadata and dimension fields null) into `quarantine_products`. |
| **Sellers** | `bronze/sellers/` | `silver/sellers/` | • Frame for Silver layer processing configured. |

---

##  Key Technical Highlights
1. **Databricks Secret Management**: Avoided hardcoded credentials by injecting storage keys dynamically using Databricks Secret Scopes (`azure-kv-scope`).
2. **Delta Lake Storage**: Utilized Delta Lake format for both Bronze and Silver layers, providing ACID compliance, schema evolution control, and efficient querying.
3. **Data Quality & Quarantine Pattern**: Rather than dropping bad records silently, corrupt or contradictory records (e.g. delivered orders missing delivery timestamps, undefined 0-value payments, empty product records) are separated into quarantine dataframes to prevent downstream contamination.
4. **Precision Data Typing**: Converted float/double fields to `decimal(12,2)` for financial calculations to prevent floating-point rounding errors.

---

##  How to Run the Project
1. **Configure Azure Key Vault**: Ensure the secret scope `azure-kv-scope` exists in Databricks and contains the key `storage-account-key`.
2. **Execute Ingestion Notebooks**: Run notebooks in `Ingestion/` to populate `abfss://bronze@secondstorage89.dfs.core.windows.net/`.
3. **Execute Transformation Notebooks**: Run notebooks in `transformations/` to process Bronze Delta tables into `abfss://silver@secondstorage89.dfs.core.windows.net/`.


Reviewed the answer sheet from yesterday's quiz.
Learned that in Databricks, regardless of the input format (CSV, JSON, Avro, etc.), the default format for write operations to managed or external storage is always Delta.
Explored new Common Table Expression (CTE) techniques, specifically writing CTEs within views. The Spark docs provide a great example of how to implement this.
Studied how Delta Lake enables ACID properties by maintaining transaction logs. Also looked into schema validation for pipelines and how to handle schema evolution when input fields change.
Completed up to Day 17 in the learning sheet.
Starting Day 18 now.
