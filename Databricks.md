# Databricks

---

## 📌 Executive Table of Contents (Index)

> [!NOTE]
> **Databricks & Azure Data Engineering Complete Master Reference**
> Comprehensive documentation covering Data Ingestion, Delta Lake, Lakeflow Jobs, Apache Spark Declarative Pipelines (SDP), Medallion Architecture, Streaming Joins, Change Data Capture (CDC), Software Engineering Practices, DevOps, CI/CD, PySpark Unit Testing, Git Integration, and Databricks Asset Bundles (DABs).

| Module # | Topic / Section | Key Focus Areas | Anchor Link |
| :-: | :--- | :--- | :-: |
| **01** | 🔌 **Databricks Connectors & Data Ingestion** | Batch Ingestion, Auto Loader, COPY INTO, Metadata Columns, Rescued Data, JSON & Variant Columns, Delta Sharing & Marketplace | [Jump to Module 1](#1-databricks-connectors--data-ingestion) |
| **02** | ⚙️ **Deploy Workloads with Lakeflow Jobs** | Job Building Blocks, Task Orchestration, Schedules & Triggers, Conditional Tasks, Failure Handling & Performance Monitoring | [Jump to Module 2](#2-deploy-workloads-with-lakeflow-jobs) |
| **03** | 🚀 **Lakeflow Jobs in Production & Best Practices** | Compute Selection, Pricing Structure, Modular Design, Git Configuration | [Jump to Module 3](#3-lakeflow-jobs-in-production-and-best-practices) |
| **04** | ⚡ **Apache Spark Declarative Pipelines (SDP)** | Data Engineering Platform Overview, Medallion Architecture (Bronze/Silver/Gold), Incremental Processing | [Jump to Module 4](#4-build-data-pipelines-with-apache-spark-declarative-pipelines) |
| **05** | 📊 **Course Project & Dataset Types Overview** | Streaming Tables, Materialized Views, Temporary Views, DLT to SDP Migration Reference | [Jump to Module 5](#5-course-project-and-dataset-types-overview) |
| **06** | 🛠️ **Apache Spark™ Declarative Pipeline Fundamentals** | Multi-File Editor, Pipeline Settings, Compute & Code Assets, Parameters | [Jump to Module 6](#6-apache-spark-declarative-pipeline-fundamentals) |
| **07** | 🛡️ **Ensure Data Quality with Expectations** | Violation Actions (`FAIL THIS UPDATE`, `DROP ROW`, `WARN`), Full SQL Implementation Examples | [Jump to Module 7](#7-ensure-data-quality-with-expectations) |
| **08** | 🌊 **Streaming Joins & Production Pipelines** | Stream-Snapshot Joins, Stream-Stream Joins, Trigger Modes, Event Log Queries & Monitoring | [Jump to Module 8](#8-streaming-joins-and-deploying-pipelines-to-production) |
| **09** | 🔄 **Change Data Capture (CDC) Overview** | SCD Type 1 (Overwrite), SCD Type 2 (Versioning / History), `APPLY CHANGES INTO` SQL Syntax | [Jump to Module 9](#9-change-data-capture-cdc-overview) |
| **10** | 💻 **Software Engineering (SWE) Best Practices** | Code Style, Documentation, PySpark Code Refactoring (Non-Modularized vs Modularized Code) | [Jump to Module 10](#10-software-engineering-swe-best-practices) |
| **11** | ♾️ **DevOps Fundamentals & Role of CI/CD** | DataOps, MLOps, CI/CD Pipelines, Testing Steps, Continuous Delivery Lifecycle | [Jump to Module 11](#11-devops-fundamentals--role-of-cicd) |
| **12** | 🏗️ **Project Planning, Environment Isolation & Testing** | Workspace & Unity Catalog Isolation, PySpark Unit Testing (`pytest`), Integration Testing | [Jump to Module 12](#12-project-planning-testing-git--dabs-deployment) |
| **13** | 🔀 **Version Control with Git Overview** | Secure Branching, GitHub PAT Setup, Databricks Git Folders & Repos | [Jump to Section](#d-generating-github-personal-access-tokenpat) |
| **14** | 📦 **Deploying Databricks Assets (DABs)** | Deployment Options, Databricks Asset Bundles (DABs), CI/CD Workflow with DABs | [Jump to Section](#14-deploying-databricks-assets-overview) |

<details open>
<summary><strong>🔍 Click to Expand Full Detailed Index & Navigation Tree</strong></summary>

- [1. Databricks Connectors & Data Ingestion](#1-databricks-connectors--data-ingestion)
  - [Databricks Connectors](#databricks-connectors)
  - [Delta Lake](#delta-lake)
  - [Data Ingestion from Cloud Storage](#data-ingestion-from-cloud-storage)
  - [Appending Metadata Column on Ingestion](#appending-metadata-column-on-ingestion)
  - [Working with Rescued Data Column](#working-with-rescued-data-column)
  - [Ingestion JSON Formatted Data & Variant Columns](#ingestion-json-formated-data)
  - [Ingest Enterprise Data Overview](#ingestin-interprise-data-overview)
  - [Data Ingestion with Partner Connectors](#data-ingestion-with-parter-connectors)
  - [Delta Sharing & Databricks Marketplace](#delta-sharing--databricks-marketplace)
  - [Ingestion into Existing Delta Table](#ingestion-into-exiting-delta-table)
- [2. Deploy Workloads with Lakeflow Jobs](#2-deploy-workloads-with-lakeflow-jobs)
  - [What is Lakeflow Jobs](#what-is-lakeflow-jobs)
  - [Building Blocks of Lakeflow Jobs](#builing-blocks-of-lakeflow-jobs)
  - [Task Orchestration](#task-orchestration)
  - [Course Project Overview](#course-project-overview)
  - [Common Task Configuration Options](#common-task-configuration-options)
  - [Job Schedules and Triggers](#job-schedules-and-triggers)
  - [Conditional and Iterative Tasks](#conditional-and-iterative-tasks)
  - [Handling Task Failures and Monitoring Jobs Performance](#handling-task-failures-and-monitoring-jobs-performance)
- [3. Lakeflow Jobs in Production and Best Practices](#3-lakeflow-jobs-in-production-and-best-practices)
  - [Common Best Practices (Compute & Pricing)](#a-common-best-practices)
  - [Modular Design in Databricks Lakeflow Jobs](#modular-design-in-databricks-lakeflow-jobs)
  - [Jobs and Git Configuration](#c-jobs-and-git)
- [4. Build Data Pipelines with Apache Spark Declarative Pipelines](#4-build-data-pipelines-with-apache-spark-declarative-pipelines)
  - [Data Engineering Platform Overview](#a-data-engineering-platform-overview)
  - [Apache Spark™ Declarative Pipelines](#b-apache-spark-declarative-pipelines)
  - [Medallion Architecture (Bronze / Silver / Gold)](#f-simplifying-batch-and-streaming-etl-in-the-medallion-architecture)
  - [Incremental Processing in Declarative Pipelines](#g-incremental-processing-in-declarative-pipelines)
- [5. Course Project and Dataset Types Overview](#5-course-project-and-dataset-types-overview)
  - [Dataset Types (Streaming Tables, Materialized Views, Views)](#b-dataset-types)
  - [What Changed from DLT to SDP (Migration Reference)](#f-what-changed-from-dlt-to-sdp)
  - [Declarative Pipeline Graph & Automatic Dependency Resolution](#g-the-declarative-pipeline-graph)
- [6. Apache Spark™ Declarative Pipeline Fundamentals](#6-apache-spark-declarative-pipeline-fundamentals)
  - [Simplified Pipeline Development (Multi-File Editor)](#a-simplified-pipeline-development)
  - [Common Pipeline Settings (Compute, Assets, Configuration)](#b-common-pipeline-settings)
- [7. Ensure Data Quality with Expectations](#7-ensure-data-quality-with-expectations)
  - [The Three Violation Actions (`FAIL`, `DROP`, `WARN`)](#b-the-three-violation-actions)
  - [Full SQL Expectation Example](#c-adding-expectations--full-sql-example)
- [8. Streaming Joins and Deploying Pipelines to Production](#8-streaming-joins-and-deploying-pipelines-to-production)
  - [Stream-Snapshot & Stream-Stream Joins](#a-what-are-streaming-joins)
  - [Scheduling, Notifications & Pipeline Event Log Monitoring](#g-schedule-notifications-and-monitoring)
- [9. Change Data Capture (CDC) Overview](#9-change-data-capture-cdc-overview)
  - [SCD Type 1 Overview & Step-by-Step Example](#b-scd-type-1--overwrite-target-with-latest-values)
  - [SCD Type 2 Overview & Step-by-Step Example](#c-scd-type-2--historical-trackingversioning)
- [10. Software Engineering (SWE) Best Practices](#10-software-engineering-swe-best-practices)
  - [Coding Practices, Code Documentation & CI/CD](#b-best-practices)
  - [Modularizing PySpark Code (Non-Modularized vs Modularized Code)](#a-modularizing-pyspark-code-non-modularized-code-before)
- [11. DevOps Fundamentals & CI/CD](#11-devops-fundamentals--role-of-cicd)
  - [DevOps Lifecycle, DataOps, MLOps](#a-what-is-devops)
  - [Role of CI/CD in DevOps](#continuous-integration-ci-and-continuous-deployment-cd)
- [12. Project Planning, Testing, Git & DABs Deployment](#12-project-planning-testing-git--dabs-deployment)
  - [Planning the Project & Workspace / Unity Catalog Isolation](#a-requirements)
  - [PySpark Unit Testing (`pytest`)](#a-unit-tests-benefits)
  - [Integration Testing with SDP and Jobs](#a-executing-integration-tests)
  - [Version Control with Git & GitHub PAT](#a-complications-with-version-control)
  - [Deploying Assets with Databricks Asset Bundles (DABs)](#a-deployment-options)

</details>

---
## Databricks connectors
* These are tools/plugins that allow databricks to talk to external systems - databases, storage systems, SaaS applications etc.
* so you can read from them or write data to them.

| Connector | Use Case |
| --- | --- |
| JDBC/ODBC Connector | Connect to relational databases (MySQL, PostgreSQL, SQL Server, Oracle, etc.) to read/write structured data |
| Azure Blob Storage Connector | Read/write files (CSV, Parquet, JSON, etc.) directly from Azure Blob Storage |
| Azure Data Lake Storage (ADLS) Gen2 Connector | Optimized for big data analytics; read/write large datasets with high performance |
| Azure Cosmos DB Connector | Read/write semi-structured data from NoSQL databases |
| Azure Key Vault Connector | Securely access secrets, keys, and certificates from Azure Key Vault |
| HTTP Connector | Fetch data from REST APIs and web services |
| File System Connector | Read/write files from the local file system or DBFS (Databricks File System) |
| SFTP Connector | Securely transfer files over SFTP protocol |
| Delta Sharing Connector | Share Delta tables securely with other Databricks workspaces or external systems |
| Unity Catalog Connector | Manage data governance and access control across the lakehouse |
| AWS S3 Connector | Read/write data from Amazon S3 buckets (for cross-cloud analytics) |
| Delta Lake Connector | Read/write Delta tables with ACID transactions and schema enforcement |
| Iceberg Connector | Read/write Apache Iceberg tables (open table format) |
| Hudi Connector | Read/write Apache Hudi tables (incremental data processing) |

<div align="center">
  <img src="Images/image%20copy.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

<div align="center">
  <img src="Images/connectors.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

### ingestion methods
* when ingesting data into databricks using  lakeflow connect standard connectors, you can choose from serveral methods.
1. batch ingestion : loads the data in batches of the rows based on the schedules, but while loading the data it load whole data every time.

<div align="center">
  <img src="Images/batch_ingestion%20.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

2.increamental batch ingestion: automaticaly detect new records in the data sources and skips the record which are already loaded in previous batches. this is fast and resource efficient.

<div align="center">
  <img src="Images/increamental_batchs.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

3. streaming ingestion : in this data is continuasly loaded as it generated, and allow use to query in real time, this method is ideal for loading data from source like apache kafka, amazon kinesis, pub/sub,and apache pulsar.


---
## Delta lake
* Delta Lake is an open-source storage layer that sits on top of your data lake (like Azure Data Lake / S3) and adds reliability, transactions, and versioning to your data files.

* the goal is to ingest the data from external source like cloud storage into delta lake as delta tables, remember delta lake is simplly open source protocal to read and write data into cloud storage.

<div align="center">
  <img src="Images/deltalake.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* under the hood , delta tables store the data in within a folder directory, within the folder directory data is stored are parquet files,and what delta adds is delta logs stored as json files along with its parquet files. these logs keeps the track of all the transactions of the data of parquet files and table versions.
* within the transation log, we now have the concept of the table states,so now if you insert, delete, update the data of your tables, delta basically add the transation(the log file) and your table stays updated and managed.
* so with the transation log you are able to easly gets views of your data,and you can able to **travel back** in time. 

<div align="center">
  <img src="Images/deltalake1.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

<div align="center">
  <img src="Images/delta_features.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

1. ACID : with the help of this multiple users can perform operation concurrently, and there will be no data loss
2. DML : we can perform operations such as insert, delete, update and merge, enabling flexible data management.
3. Time travel : we can query and rever prev versions of the data, facilitating auditing and recovery.
4. Schema Enforcement and Evolution : define schema for data integrity, delta tables will validate the data before writing into the tables,while evolutin mean we can add new columns in the table without breaking workflow.

<div align="center">
  <img src="Images/medalian_architecture.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* As you ingest the data in to your delta lake through batch or streaming ingestion,or both . then you can begin processing and transforming your data into databricks.
* it begins with bronze layer, the row data ingestion layer,this layer ingest the row and unprocessed data from different data sources,and serving the fundamental storage for all data.
* in silver layer the data is cleaned, transformed and enriched and provied more refined dataset for later analysis.
* later gold layer contains business specific data, aggregated, and ready for business decision. 

---
## Data ingestion from cloud storage

* data ingested from claud storage, row files are effieciently  converted into delta tables using databricks tools.

<div align="center">
  <img src="Images/data_ingestion.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* we discussed about 3 methods of the data ingestion from cloud storage into delta tables.
    1. create table as(ctas)
    2. copy into 
    3. auto loader

 
1. **create table as(ctas)**:
   * this stmt creates a delta table by default from files stored in claud storage.

<div align="center">
  <img src="Images/ctas.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

   * the read_files() function  is used read files from specific locaion(claud storage) and return the data in table format, it offers several capabilities.
     1. support varias file format
     2. automatically detect file format and and infer unified schema across all files.
     3. can be used in streaming tables to increamently ingest files into delta lake using auto loader
2. **copy into** :
    * this command performs bulk load from files in cloud storage into the tables.

<div align="center">
  <img src="Images/copyinto%20.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

    * means copy into skips any files that already been loaded into the table, and only new files will be ingested .
    * format_option() : lets you control the control behavior of the copyinto operation itself,for example schema evaluation using mergeSchema.
3. **Auto Loader** : 
    * auto loader increamently and effiecently load new data files in either in batch or streaming mode as they arrives in cloud object storage,and it does this without any additional steps.

<div align="center">
  <img src="Images/auto_loader.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

<div align="center">
  <img src="Images/autoloader1.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* cloudfiles.format : it tells auto loader to what type of files to expect in the directory, such as csv, json, parquet, etc. This setting is necessary for the auto loader to properly parse and process the files.
* schemalocation:save or remember the structure(schema) of the data here, so i dont re detect it every time.
* trigger( time = x seconds): check for new files every 5 second and process them - the hearbeat interval is the time gap between each check. 
* to create streaming table from the files in volume you use auto loader , databricks recommands using auto loader with lakeflow declrative pipelines for most of data ingestion tasks from the cloaud storage
* streaming table : this is delta table that continously updates itself as new data is ingested into it, and allows you to query the data in real-time.
* streaming tables in databricks sql are backed by serverless lakeflow declarative pipelines. your workspace must support serverless piplines to use this functionality. alternatively, use can build your own lakeflow declarative pipelines for incremental processing, optimzation and monitoring. 

<div align="center">
  <img src="Images/ingestion_summary.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

---

## Appending metadata column on ingestion

<div align="center">
  <img src="Images/meta_data_image.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* you can append metadata column info  from input data source files when creating table .
* this is very important for tracking the info of table ,auditing,lineage, and debugging purpose.

## working with rescued data column

* during ingestion there are times when input data does not match with schema in your table. ingestion technique like read_file(),spark.read, autoloader provide rescued sdata column during ingestion.this make sure that incoming data which fail in schema validation is not lost.

<div align="center">
  <img src="Images/rescued_data.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

<div align="center">
  <img src="Images/rescued_data1.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* when we ingest data then user column must read as string in table and cost column must be in Bigint 
* see in first row user column passed the validation but cost column contain value as string like $100 which is failed to load because expection is like that in bigint
* so instead of simply droping that column simple we add new column name rescued data where we add this cost as json format and actual cost column filled with null value.

## Ingestion json formated data

<div align="center">
  <img src="Images/Screenshot%202026-06-10%20at%206.43.25 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* we deal with this type of data commanly when dealing with event data, logs, data from apis
* these object can be flat means all the key: value pair can be in single level,or they can be in nested structure, where value also can be in nested structure.

<div align="center">
  <img src="Images/Screenshot%202026-06-10%20at%206.46.39 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* its common that after ingestion one or more column in your table might cantain json-format string as values.
* so the techniques to parse these json objects,extract,manipulate these json string using sql or dataframe operation.so that you can access these key value pair like a common column field.

<div align="center">
  <img src="Images/Screenshot%202026-06-10%20at%206.50.26 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* a key point to remember:
    1. a column can store json string or json object as string.
    2. so its just row text from system perspective.
* to access subfield from json formated string column, you can use the colon(:) syntax.

<div align="center">
  <img src="Images/Screenshot%202026-06-10%20at%206.54.26 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* lets go through the process of mapping json formatted string into struct column 
    * so the first step to define json formatted schema of json formatting string .
    * defining the schema allow you to tell databricks how to interpret each part of json string and how to convert these into appropriate data type within a struct.

<div align="center">
  <img src="Images/Screenshot%202026-06-10%20at%206.58.28 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

<div align="center">
  <img src="Images/Screenshot%202026-06-10%20at%207.00.00 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* these can be done with these 2 steps:
    - first step is to get schema of the json format string
    - now instead of manually defining schema, you can use builtin schema_of_json() function to automatically define the schema of the example json string.
```sql
select schema_of_json(json_col, 'json-struct-schema') as struct_column
from table;
```

<div align="center">
  <img src="Images/Screenshot%202026-06-10%20at%207.13.10 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* some major benifits of variant  data type includes:
    - it can store any type of data including json, making ideal for semi-sturctured data.
    - it is highly flexible, it can adapting to different data shapes without rigit schemas
    - if offers improved performace as compared to existing methods for handling semi-structred data.
* working with **variant column(Public preview)**:
    - full open source,no strict schema.
    - you can put any type of semi-structured data into variant column.
    - improved performance over traditional methods.
    - this will not work in serverless compute.

### Parsing and Querying Variant Columns

1. **Parsing JSON into Variant**:
   Use `parse_json()` to convert a raw JSON string into a binary `VARIANT` data type.
```sql
   SELECT parse_json('{"user": "Anil", "age": 28, "skills": ["SQL", "Spark"]}') AS my_variant;
   ```

2. **Querying Fields using Colon (`:`) Operator**:
   You can extract values using the colon `:` operator without any schema definition:
   * **Top-Level Field**: `my_variant:user`
   * **Nested Field**: `my_variant:details.city`
   * **Array Element**: `my_variant:skills[0]`

3. **Casting Variant Fields to Primitive Types**:
   By default, path traversal on a `VARIANT` returns another `VARIANT`. To get a regular data type (like string or int), cast it using `::` or `cast()`:
```sql
   SELECT 
       my_variant:user::string AS username,
       cast(my_variant:age AS int) AS age
   FROM table;
   ```

4. **Functions for Parsing and Retrieval**:
   * **`variant_get(col, path, target_type)`**: Extract value with a JSON path (e.g., `'$.user'`). Fails if cast fails.
   * **`try_variant_get(col, path, target_type)`**: Safer version. Returns `NULL` if path does not exist or if cast fails, rather than raising an error.
```sql
   SELECT try_variant_get(my_variant, '$.skills[0]', 'string') AS first_skill;
   ```

---

## Ingestin interprise data overview

* connection interprise data is streamlined with lakeflow connect managed connectors and partern connect enabling fast and reliable data integration from databases and application into databricks lakehouse- with fully managed and flexible options.

<div align="center">
  <img src="Images/Screenshot%202026-06-11%20at%204.34.39 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- so far we have discussed how to ingest data from cloud storage using ctas, copy into and autoloader but what if data stored in databases, and external applications?

<div align="center">
  <img src="Images/Screenshot%202026-06-11%20at%204.37.24 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- first method of ingest data from interprise application is lakeflow connect managed connetors.
- this simply the process of ingesting data from varity of the interprise databases and applictions.
- with low code, fully managed experience to connect, ingest and synchronize data from external sources into the databricks lakehouse.
- it also provide easy to use UI for user and also provide API .

<div align="center">
  <img src="Images/Screenshot%202026-06-11%20at%204.37.24 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- these are highly efficient , databricks managed connectors designed specially for fast, reliable ingestion into your lakehouse.

<div align="center">
  <img src="Images/Screenshot%202026-06-11%20at%204.44.44 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* how it works:
    - a lakehouse declarative pipelines job collects credentials from unity catalog.
    -  the service transform the data and store it into streaming delta table.
* its primary role is to connects to public SAAS based sources(salesforces,workdays..) extract the data and ingest it into streaming table.

<div align="center">
  <img src="Images/Screenshot%202026-06-11%20at%204.50.46 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- like with public saas connectors, this architecture is designed to move data into streaming table- but this time from external databases rather than external API.
* how it works:
    - A classic compute  declarative pipelines job retrives credentials securely from unity catalog.it connect to data using JDBC and retrive data from source table in batches.
    - it use those credentials to connect with external databases.
    -  the jobs collect latest state and change the logs and storing staged data into unity catalog volumn.
    - serverless declarative pipelines job then process staged data and load it into streaming delta table.

* We are introducing two new architectural elements are:
    * ingestion :
            * a dedicated pipelines that connect database to extract : **metadata, snapshot, change logs**
    * unity catalog volumn:
            * this act as intermediate staging layer, enabling the next pipeline to pick up and stream data.
            * its secured using standard UC mechanism, and by default access is limited to users running the pipeline.
            
## data ingestion with parter connectors

* if there is not managed connector availabe for your specific data source, for that you can also use partner connect.
* with partner connect you can get a list of all the available partner connectors, and you can use them to ingest data from your data source to your lakehouse.

<div align="center">
  <img src="Images/Screenshot%202026-06-11%20at%205.19.22 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

---
## Delta Sharing & Databricks Marketplace

* Databricks provides a secure, open, and efficient way to share data and AI assets both internally and externally through **Delta Sharing** and the **Databricks Marketplace**.

### 1. Delta Sharing (The Protocol)
* **What is it?** An open-source protocol developed by Databricks for secure, real-time data sharing across different organizations and platforms without replicating or copying data (zero-copy).
* **Key Features:**
    * **Open Protocol:** Recipients do not need to use Databricks. They can query shared datasets using standard tools like Power BI, Tableau, pandas, Apache Spark, or Python.
    * **Zero-Copy & Live Data:** Data is shared directly from cloud storage (like ADLS Gen2/S3). Recipients always query the latest "live" version of the data, eliminating outdated static file dumps.
    * **Security & Governance:** Fully integrated with **Unity Catalog** to provide centralized auditing, access control, and usage tracking.
    * **Cross-Cloud & Region:** Share data across different cloud providers (Azure, AWS, GCP) and regions seamlessly.

### 2. Databricks Marketplace (The Exchange Hub)
* **What is it?** A public or private exchange forum built on top of Delta Sharing that allows providers to package, publish, and distribute data products and AI assets, and consumers to discover and access them.
* **Beyond Just Raw Tables:** Unlike traditional data marketplaces, Databricks Marketplace supports sharing a variety of assets:
    * **Data Products:** Structured and semi-structured datasets.
    * **AI/ML Assets:** Pre-trained machine learning models and LLM agent skills.
    * **Analytical Assets:** Databricks Notebooks, dashboards, and complete custom applications.
* **Clean Rooms Integration:** Facilitates secure, privacy-preserving collaborations where two or more parties can analyze sensitive datasets together without exposing raw data to each other.
* **No ETL Required:** Consumers can instantly mount and query published assets without setting up complex ingestion or replication pipelines.

## Ingestion into exiting delta table

<div align="center">
  <img src="Images/Screenshot%202026-06-12%20at%203.15.21 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* there are situations where you need to update, insert, delete records in target table based on info of another table.

<div align="center">
  <img src="Images/Screenshot%202026-06-12%20at%203.20.14 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

```sql
    MERGE INTO target_table target
    USING source_table source;

    -- specify the condition for merging
    MERGE INTO target_table target
    USING source_table source;
    ON target.key = source.key
```

---

# 1. Deploy workloads with Lakeflow Jobs

* it all begins with optimized storage with delta lake, parquet or iceberg.
* buit on top of this storage layer is unfied governance with unity catalog. unity catalog is centerized data catalog that provide access control, auditing, data quality, data lineage and data discovery over databricks workspace.
* databricks then offer lakeflow. an end to end data engineering solution that empowers data engineers,software developer, sql developers,analytics and data scientist to build reliable, scalable and maintainable data pipelines.this provides unified platfrom for data ingestion, transformation, orchestrations and monitoring.
    - **lakeflow connect** : A set of efficient ingestion connectors that simplify data ingestion from popular saas applications and databases, cloud storage,message buses and local files.
    - **lakeflow declarative pipelines(LDP)** : A framework for building batch and streaming data pipelines using sql and python, designed to accelerate **ETL** development.
    - **Lakeflow jobs** : A workflow automation tools for databricks that orchestrates data processing workloads. it enables coordination of multiple tasks within complex workflows, allowing for the scheduling, optimization, and management of repeatable processes.

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%203.21.45 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

## What is lakeflow jobs?

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%203.23.55 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* this diagram captures a fundamentals challenge in modern data architecture. choosing the right orchestration approach for lakehouse. the left panel shows multiple options including open source solutions(Apache airflow, Perfect, Dagster,dbt), cloud native services(aws,azure, google cloud), and custom in house frameworks.

* diagram on right illustrate a typical data workflow with multiple steps: ingesting sessions and clicks data,joining them , performing featurization and aggregation, analysis and training models, then you have various downstream uses including BI & data warehousing, Data streaming and data science & ML.

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%203.33.49 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* many orgs uses external orchestration tools, but this creates significant challenges, data teams becomes less productive because these tools are hard to use for many practitioners, bad data quality lower the value of downstream applications.

* You also face higher costs of ownership and lower reliability, when issue occur, it's difficult to understand root cause. The complex architecture become hard to manage and maintain.

* most importantly these external tools are not inified with your lakehouse creating integretion challenges and data silos.

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%203.40.00 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* This is where lakehouse jobs comes in, it provides unified orchestration for data, analytics and ai workloads directly on the data intelligence platform. 

* The key benifits are simple authoring, actionable insights,and proven reliability, because it's native to the plateform it integrates seamlessely with data ingestion & transformation the processing engine(photon), governance( UC), storage(delta lake), data warehousing,and machine learning capabilities .

* The workflow shown here - from sessions and clicks through join, featurize, aggregate, and train - all runs natively within the same platform eliminating the integration challenges of external tools.

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%203.53.04 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

*  this slides shows the complete arichitecture of lakeflow jobs, at the center is the workflow engine that coordinates everythings.
* The compute layer support various workload types: ETL,ML/AI, and analytics/BI operations.
* you have multiple trigger types: scheduled(time based), continous(always-runnnig) file arrival(event-driven), table updates.
* two critical components support the entire system: obeserability for monitoring and troubleshooting and control flow for managing task dependencing and execution order.


## Builing Blocks of lakeflow jobs

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%204.15.58 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* here are the two most fundamental concepts you need to understand:
*  a job is the primary resource for scheduling , coordinating and running operations such as data processing, etl, analytics,and ML workloads within the databricks enviroments, think of a job as the container that holds your entire workflow.

* A task is a single unit of work within a job that executes a specific workload such as a notebook, script, query and more, task are the individual building blocks that do the actual work.

* The relationship is hierarchical: each job cosists of one or more tasks, which are individual units of work that make up the job. the visual shows this clearly - one job containing multiple tasks.

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%204.23.24 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* jobs consist of one or more tasks, and there are many different task types available, you can use databricks notebook in any supported lang. , python script,python wheels for packaged code, sql queries, dbt models, java jar files,spark submit jobs for lagacy spark application, AI/BI dashboards for visulisation, and even power BI integration.

* This varity ensure that you can orchestrate virtually any type of workload within your job .

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%204.26.57 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* when you create a job, you can set specific configurations for each particular task, the options available depends on the task type you select, common confiquration option include defining the path to your code, adding libraries, setting parameters, enabling notifications, and configuring  retry policies.

* these configurations allow you to better orchestrate each particular task according to your specific needs.

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%204.31.11 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* here a specific example for notebook tasks, when you select notebook task type, you get options like specifying the source path to your notebook, choosing compute option( cluster confi.)and many more settings specific to running notebooks.
* the interface adapts based on your taks type selection, providing relevant config. options.

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%204.40.06 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* similarly ,for sql tasks , you get different options you can specify the aql task details, write or reference your sql query and select the sql  warehouse that will execute your query.

* each task type provides the specific configuration options tailored to that task type.

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%204.42.31 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* we already learned about jobs and task .
* this comprehensive view shows how tasks can be connected with different control flow pattern, you can implement sequential , paralel execution, conditional logic, fan-in/fan-out pattern , run job tasks for modular design and for each loop for iterative processing.

* additionally jobs supports different trigger types: manual triggers for on demand executions, scheduled triggers using cron expressions, api triggers for programmatic exacution, file arrival triggers for event driven processing table triggers for data change events,and continous triggers for streaming workloads.

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%204.52.06 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* jobs can be exuecuted on different types of compute and choosing the right compute is crucial for both performance and cost.

* interactive cluster can be shared by multiple users and are best for ad-hoc analysis ,data exploration or development, however they should not be used in production as they are not cost effective.d
* job clusters are approximately 50% cheaper as they terminate when the job ends, reducing resource usage and costs.  they are ideal for production workloads, through they are subject to cloud provider start up times .with databricks jobs, you can reuse the same cluster across task for better price performance.

* serverless provide a fully managed service that is operationally simpler and more reliable it offer faster cluster and auto scaling capabilities providing better user exp. for lower cost, with out of the box performance optimizatios, serverless providers lower overall TCO.

* sql warehouse is purpose built for sq queries, dashboard, and BI and is serverless by default , it offer high concurrency and autoscaling vai intelligent workload management, with auto-start/auto-stop and adjustable cluster sizing to help control costs.

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%206.17.40 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* serverless tasks,you can use the performance optimzed setting to choose between lower cost and faster execution.

* standard mode, focuses on cost-efficiency with longer startup time(typically 4-6minutes), making it best for non-urgent workload with flexible timing.

* optimized mode enables faster job , startup and execution, making it ideal for time-sensitive workloads,this setting applies only to tasks with serverless compute in your job.

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%206.21.21 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* this diagram illutrates an important principle: a job can have one or more task insider it and each task can be assigned its own compute resource. tasks in the same job can either share the same compute or use different compute as required .

* you might have task-1 running on an all purpose cluster, task -2  on serverless, task-3 on a job cluster, and so on. this flexibility allows you to optimize cost and performance for each task individually.

---

## Task Orchestration

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%206.25.30 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* A dag is a conceptual representation of a series of activities, including data processing flows, lets break down the acronym.
    - Directed means there's an unambigous direction of each edges- tasks flow in a specific direction
    - Acyclic means it contains no cycles- you can't have circular dependencies where task A depends on task B which depends on task A.
    - Graph means it has a collection of vertices connected by edges - in our case, vertices represent tasks and edges represent the dependencies between them.

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%206.30.56 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* databricks jobs support task orchestration through the ability to run multiple tasks as a directed acyclic graph(DAG). you can orchestrate tasks using the databricks UI, API, SDK or databricks asset bundles.

* the example shows that task2 depends  on task1 , task3 also depend on task 1 and task4 depends on both task2 and task3. you define the order of execution by configuring these task dependencies , creating a DAG of task execution

* this approach allows you to build complex workflows while maintaining clear dependencies and execution order.

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%206.41.42 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* there are 3 approach to this:
    - sequence pattern is used for data transformation, processing, cleaning, and building bronze/silver/gold  table in a medallion architecuter.
    - funnel pattern brings together multiple data sources for data collection and consolidation.
    - fan-out or star pattern takes a single data source and distributes it for data ingestion and distribution to multiple downstream systems.

    
## Course Project Overview

* in this section you will get an overview of  the course project. where you will build a retail pipeline from a retail dataset using different tasks available in lakeflow jobs.

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%206.52.43 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* this diagram shows our complete course project architecture, we will build a retail data processing pipeline that demonstrates all the concepts we are learning.

* starting with retail data in cloud storage, we will ingest customer,sales and orders data using different task types, we will join customers and orders and customers and sales using notebook tasks, the workflow includes an if/else block for checking duplicates with true and false conditions.

* we will implementa for each task for state - wise iteration on customer orders data. finally we will create a retail dashboard using a dashboard task.

* this project incorporates sql tasks, notebook tasks, if/else logic, iterative processing and dashboard creating - giving you hand-on exp. with all major  lakeflow jobs features.


---
## Common Task configuration Options

 * lets start with the common configuration options that you apply to your tasks. These settings are key to building workflows that are not just automated, but also dynamic,context-aware and easy to monitor.

 * we all how to pass values into your tasks and how to set up alerts.

<div align="center">
  <img src="Images/Screenshot%202026-06-15%20at%209.26.06 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

 * lets me walk you through the three major categories of task configuration options:

 * parameters & dynamic values refereces are the foundation of flexible workflows. these can only be set at the task level and allow you to create reusable ,adaptable tasks that can behave differently based on the context or input.

 * retries are your first line of defense against transient failure. you can configure retry behavior at both job and task levels. allowing for different retry strategies depending on the criticality and expected failure pattern of different parts of your workflow.
 * notification alert keeps your team informed and enable rapid response to issue. like retries, these can configured at both job and task levels, giving you granular control over who gets nofified about what events.

<div align="center">
  <img src="Images/Screenshot%202026-06-16%20at%2012.34.50 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

 - understanding the parameter hierarchy is crucial for effective job design:

    - task parameters are key - value pairs or json array defined at the individual task level. these are specific to each taks and allow for fine-grained control over task behavior.
    - job parameter are defined at the job level and automatically propogate to all tasks within that job. this create a powerful inheritance model where you can set common defaults while still allowing task specific overrides.
- The precedence rule is critical to remember: job parameters always override task parameter when the same key exits. this design pattern allow you to establish sensible default at the job level while manintaining the flexibility to customize individual tasks when needed.

<div align="center">
  <img src="Images/Screenshot%202026-06-16%20at%2012.43.04 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- Task parameters are far more than simple configuration values they are the building blocks of intelligent workflows. these key value pairs enable sophisticated orchestration pattern:
    * conditional execution : use parameters to control which branch of your workflwo execute based on data conditions, env. settings, or business rules.
    * looping : parameters can control iteration counts, define arrays for for-each loop , and manage complex processing scenarios.
    * context passing : share info between tasks by setting parameters that downstream task can read, creating a data flow alongside your control flow.

- the real power comes from combining parameters with dynamic values references, allowing your workflows to adapt intelligently to changing conditions and data characteristics.

<div align="center">
  <img src="Images/Screenshot%202026-06-16%20at%2012.55.28 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- Job parameters serve as the foundation for consistent, maintainable workflows. they are key-value pairs that provide default values to for your entire workflow, ensuring consistency across all tasks.

- Here's what makes them powerfull : 

    * Automatic application : every task in the job automatically receives these parameters, eliminating the need to manually configure common settings across multiple tasks.
    * Override capability : tasks can still define their own parameters with the same key names. but job parameters take precedence, giving you centralized contorl.
    * Runtime flexibility : you can override job parameters when triggering job runs, allowing the same job defination to behave differently for different scenarios - perhaps different env.,date ranges, or processing modes.

<div align="center">
  <img src="Images/Screenshot%202026-06-16%20at%201.15.12 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

<div align="center">
  <img src="Images/Screenshot%202026-06-16%20at%201.11.10%20PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- let's look at the practical implementation of parameters:

    * **Setting Job Parameters:** navigate to your job's Parameters section and add key-value pairs at the job level. these become available to all tasks automatically. common examples include catalog names, schema names, environment settings, and processing dates.

    * **Setting Task Parameters:** within each task's configuration (found alongside task name, type, and path settings), add task-specific key-value pairs. these are perfect for task-specific paths, processing options, or override values.

    * **Retrieving in Notebook Tasks:** use `dbutils.widgets.get("parameter_name")` to access both job and task parameters. the system automatically handles the precedence - if both job and task parameters exist with the same key, you'll get the job parameter value.

    * **Language-Specific Retrieval:** remember that parameter retrieval methods vary by task type. SQL tasks access parameters differently than Python wheel tasks or JAR tasks. always check the documentation for your specific task type.

<div align="center">
  <img src="Images/Screenshot%202026-06-16%20at%202.57.33 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- Dynamic Value References using `{{ }}` notation unlock powerful runtime capabilities that make workflows truly adaptive:

- **Job Context References:**
    * `{{job.start_time.day}}` - access execution timing for date-based processing
    * `{{job.run_id}}` - unique identifier for tracking and logging
    * `{{job.parameters.environment}}` - access job-level parameters dynamically

- **Task Context References:**
    * `{{task.name}}` - useful for logging and dynamic path generation
    * `{{task.retry_count}}` - track retry attempts for debugging

- **Inter-Task Communication:**
    * `{{tasks.data-validation.values.record_count}}` - access computed results from upstream tasks
    * `{{tasks.file-processor.values.output_path}}` - use dynamic paths generated by other tasks

- **Advanced Patterns:** these references enable workflows that adapt to different execution environments, process varying data volumes, and make intelligent decisions based on upstream results.

<div align="center">
  <img src="Images/Screenshot%202026-06-16%20at%203.01.08 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- Notification alerts form a critical part of operational excellence, and understanding the configuration levels helps you build effective alerting strategies:

- **Job Level Notifications:** configure these in the Job Details section's right-side pane. job-level alerts are sent after the entire job completes successfully. this is perfect for stakeholders who need to know when complete workflows finish, such as business users waiting for daily reports or downstream systems that depend on your job's outputs.

- **Task Level Notifications:** each task can have its own notification configuration, allowing granular alerting strategies. this is essential when different tasks have different stakeholders or when certain tasks are more critical than others. for example, you might want immediate alerts for data validation failures but only summary notifications for routine cleanup tasks.

- **Strategic Considerations:** design your notification strategy based on operational needs, not technical convenience. consider who needs to know what, when they need to know it, and what actions they can take based on the notification.

<div align="center">
  <img src="Images/Screenshot%202026-06-16%20at%205.33.31 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- modern production environments require sophisticated notification strategies:

- **Multiple Destinations:** support for Emails, Microsoft Teams, PagerDuty, Slack, and Webhooks means you can integrate with your existing operational tools and communication patterns. different teams might prefer different channels - developers might want Slack notifications while operations teams prefer PagerDuty integration.

- **Per-Task Customization:** each task in a job can have completely different notification configurations. your data ingestion tasks might send alerts to the data engineering team, while your reporting tasks notify business stakeholders.

<div align="center">
  <img src="Images/Screenshot%202026-06-16%20at%205.35.58 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- a well-designed retry policy is essential for resilient workflows. the policy determines not just how many times to retry, but under what conditions and with what timing patterns.

- consider factors like:
    * **Failure Type:** transient network issues might warrant immediate retries, while data quality issues might not
    * **Resource Impact:** retrying resource-intensive tasks too aggressively can cause cluster resource contention
    * **Downstream Dependencies:** failed tasks might impact other workflows, making retry timing critical
    * **Business SLA:** some processes have strict timing requirements that limit retry windows

## Job Schedules and Triggers

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%201.20.17 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- a trigger is fundamentally a rule engine that automatically initiates job execution based on specific conditions or schedules. this isn't just about convenience - it's about building reliable, responsive data systems that can operate autonomously.

- **Trigger Categories:**
    * **Time-based schedules:** traditional cron-style scheduling for predictable, recurring workloads
    * **Continuous execution:** always-on processing for streaming data scenarios
    * **File arrival events:** event-driven processing that responds immediately to new data
    * **Manual triggers:** on-demand execution for development, testing, and ad-hoc analysis
    * **Table Update:** for enabling automated job execution as soon as specified tables are updated


<div align="center">
  <img src="Images/Screenshot%202026-06-16%20at%205.40.53 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- a **trigger** is a rule that automatically starts a job run based on a **specific condition** or **schedule**.
- common trigger types include:
    1. Time-based schedules
    2. Continuous (always-on) execution
    3. File arrival events
    4. Manual trigger
    5. Table Update
- triggers **enable automation**, so jobs can run without manual intervention.

<div align="center">
  <img src="Images/Screenshot%202026-06-16%20at%205.43.18 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- scheduled triggers are the backbone of most production data workflows, providing reliable, time-based execution:

- **UI-Based Scheduling:** the Databricks interface provides intuitive scheduling options for common patterns - hourly, daily, weekly, monthly. this is perfect for business users and reduces the learning curve for cron syntax.

- **Cron Expression Power:** for more complex timing requirements, full cron expression support enables sophisticated schedules like "every 15 minutes during business hours" or "first Monday of each month."

- **Use Case Patterns:**
    * **Daily ETL:** process yesterday's data every morning at 6 AM
    * **Weekly Reports:** generate executive dashboards every Monday morning
    * **Monthly Aggregations:** calculate monthly KPIs on the first day of each month
    * **Hourly Streaming Checkpoints:** regular maintenance for streaming jobs

- **Timezone Considerations:** always specify the appropriate timezone for your business context, especially for organizations operating across multiple regions.

<div align="center">
  <img src="Images/Screenshot%202026-06-17%20at%203.39.39 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- file arrival triggers represent a paradigm shift from time-based to event-driven processing, enabling immediate response to data availability:

- **Storage Platform Support:** comprehensive support across AWS S3, Azure Storage, Google Cloud Storage, and Databricks Volumes ensures you can implement event-driven patterns regardless of your cloud platform.

- **Event-Driven Architecture:** this trigger type enables true event-driven data architectures where processing begins immediately when data becomes available, rather than waiting for the next scheduled execution.

- **Real-World Scenarios:**
    * **Partner Data Feeds:** process files as soon as external partners upload them
    * **IoT Data Processing:** handle sensor data files uploaded irregularly throughout the day
    * **Financial Data:** process trading data files that arrive at unpredictable intervals
    * **Log File Processing:** handle application logs uploaded by various systems

- **Pattern Matching:** configure sophisticated file pattern matching to ensure you process only relevant files and ignore temporary or incomplete uploads.

<div align="center">
  <img src="Images/Screenshot%202026-06-17%20at%203.43.29 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


- continuous triggers are designed specifically for workloads that need to maintain constant processing:

- **Automatic Restart Logic:** built-in retry logic automatically managed by Databricks ensures that streaming jobs maintain continuity even through transient failures.

- **Resource Management:** continuous jobs are automatically managed to prevent resource leaks and ensure optimal cluster utilization over extended periods.

- **Streaming Use Cases:**
    * **Real-time Analytics:** continuous processing of clickstream data for real-time dashboards
    * **Fraud Detection:** always-on processing of transaction streams for immediate fraud identification
    * **IoT Processing:** continuous ingestion and processing of sensor data streams
    * **Change Data Capture:** real-time processing of database change streams

- **Monitoring considerations:** continues jobs required different monitoring appraoch since they are designed to run indefinitely rather than complete discrete task.

<div align="center">
  <img src="Images/Screenshot%202026-06-17%20at%203.55.30 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- manual triggers provide essential flexibility for development, testing, and ad-hoc processing scenarios:

- **Execution Options:**
    * **UI Execution:** "Run now" for immediate execution with current settings
    * **Parameterized Execution:** "Run now with different settings" allows runtime parameter overrides
    * **Programmatic Execution:** API, CLI, SDK, and Databricks Asset Bundles enable integration with external systems

- **Development Workflow:** manual triggers are essential during development for testing job logic, debugging issues, and validating changes before implementing automated triggers.

- **Operational Use Cases:**
    * **Backfill Processing:** handle historical data processing outside normal schedules
    * **Emergency Processing:** respond to urgent business needs that can't wait for scheduled execution
    * **Data Recovery:** reprocess specific time periods after resolving data quality issues
    * **Testing and Validation:** verify job behavior in production environments before enabling automation

<div align="center">
  <img src="Images/Screenshot%202026-06-17%20at%204.01.12 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


- here we're introducing the **Table Update Trigger** — a new trigger type in Databricks Lakeflow Jobs.
    * it automatically starts a job whenever specified source tables are updated.
    * this means we no longer rely on manual or cron-based schedules. instead, jobs run in real time — as soon as new data lands — which improves freshness and reduces wasted compute.
    * it works by monitoring one or more tables for any data change — such as insert, update, delete, or merge.
    * you can configure it easily by selecting the 'Table update' option within job triggers, and then listing the tables you want to watch.

<div align="center">
  <img src="Images/Screenshot%202026-06-17%20at%204.05.28 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- let's walk through how it works:
    1. you first select **Table Update** as the trigger type.
    2. then add your source tables. you can include up to ten tables in a single trigger, supporting Unity Catalog–managed Delta or Iceberg tables, materialized views, and streaming tables.
    3. then, define when the trigger should fire:
        * it can run when *any* of the listed tables change,
        * or wait until *all* of them are updated.
    4. finally, we have advanced options for more control —
        * **Minimum time between triggers** places a buffer between runs to prevent over-triggering during rapid table updates. example: for a frequently updated table, set a gap between consecutive runs to avoid multiple job executions in quick succession.
        * **Wait after last change** delays the job until a set period has passed since the most recent update, ensuring all data has landed. example: when data arrives in multiple batches, define a waiting period so the job starts only after the entire batch is delivered.

- together, these settings make orchestration smarter, more reactive, and resource-efficient.

---
## Conditional and Iterative tasks


* we are not entering the realm of intelligent workflows that can make decision and adapt their behavior based on runtime conditions. these are not just linear seq. of task - they are dynamic workflows that can branch, loop , and make intelligent decision based on data and processing results.

* this capability transform your workflows from simple automation to intelligent data processing systems.

- three advanced task types enable sophisticated workflow patterns:

- **Run-if Conditional Task Dependencies:** control task execution based on the outcomes of upstream tasks, enabling workflows that can handle partial failures and complex dependency scenarios.

- **If/Else Tasks:** implement boolean conditional logic directly in your workflow, allowing branches based on data conditions, processing results, or business rules.

- **For Each Tasks:** enable iterative processing patterns where the same logic is applied to multiple data partitions or parameters, with configurable parallelism for performance optimization.

- these task types can be combined to create sophisticated workflows that handle complex business logic while maintaining clarity and maintainability.


<div align="center">
  <img src="Images/Screenshot%202026-06-17%20at%204.17.16 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- Run-if conditional dependencies provide fine-grained control over task execution based on the outcomes of upstream tasks. you can define specific conditions that must be met for a task to run.

- **Available Dependency Conditions:**
    * **All succeeded:** this traditional dependency requires all upstream tasks to complete successfully before the next task can run.
    * **At least one succeeded:** this condition is useful when you have redundant data sources or processing paths, allowing the workflow to proceed if even one of the required upstream tasks is successful.
    * **None failed:** this allows a task to execute even if some upstream tasks were skipped, as long as no upstream tasks have explicitly failed.
    * **Custom combinations:** this option enables the implementation of complex business logic that requires specific combinations of task outcomes.

-  **Benifits of conditional dependencies:**
    * These dependencies enhance workflow resilience and enable sophisticated business logic. for instance, if task -4 is set to run when "atleast one" of its predecessors(task2,3) succeeds , it still executed if task3 fails while task2 complete successfully. this prevents cascade failers and allows workflows to continue process even when some components fail. it also help mirror real world buisness processes where multiple paths to seccesss exits and partial failure do not halt the entire operation.
- **How to configure:**
    * you can select these dependency conditions for a task within its configurations settings unders the run if dependenies section. which we are going to see next.


<div align="center">
  <img src="Images/Screenshot%202026-06-17%20at%205.35.54 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- the visual representation of conditional dependencies in the job DAG provides immediate understanding of workflow logic:

- **Dependency Visualization:** different line styles and colors indicate different dependency types, making complex logic easy to understand at a glance.

- **Troubleshooting Benefits:** when failures occur, the visual representation immediately shows which tasks were affected and which could continue, accelerating root cause analysis.

- **Team Communication:** visual workflows serve as living documentation that both technical and business stakeholders can understand, improving collaboration and change management.


<div align="center">
  <img src="Images/Screenshot%202026-06-17%20at%205.38.05 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- if/else tasks enable direct implementation for business logic within your workflow,moving beyond simple  success/failure condition of data driven decision making.

<div align="center">
  <img src="Images/Screenshot%202026-06-17%20at%205.39.35 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- if/else conditional tasks add sophisticated boolean logic to workflows:

- **Condition Evaluation:** boolean operators (==, !=, >, >=, <, <=) evaluate expressions against task results, parameter values, or computed metrics.

- **Business Logic Examples:**
    * **Data Quality Gates:** branch based on record counts, null percentages, or validation results
    * **Processing Volume Decisions:** use different processing strategies for large vs. small datasets
    * **Environment-Specific Logic:** execute different tasks based on environment parameters
    * **Business Rule Implementation:** implement complex business rules directly in workflow logic

- **Execution Requirements:** the condition "If none of dependency failed and at least one task executed" ensures that conditional evaluation only occurs when meaningful upstream results are available.

- **True/False Branches:** each branch can contain multiple tasks, enabling complex processing paths based on conditional outcomes.

<div align="center">
  <img src="Images/Screenshot%202026-06-17%20at%205.43.11 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

* for each task enable powerfull iterative processing patterns  that maintain the benifits of visual workflow management while handlingi repetitive operations efficiently.

<div align="center">
  <img src="Images/Screenshot%202026-06-17%20at%205.44.36 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- For Each tasks provide sophisticated iteration capabilities:

- **Input Processing:** the task loops over an input array, passing each item as `{{input}}` to the nested task. this creates clean, parameterized processing where the same logic handles different data partitions.

- **Parallel Execution:** configurable concurrency allows multiple iterations to run simultaneously, dramatically improving performance for independent processing tasks.

- **Dependency Management:** downstream tasks depend on the completion of the entire For Each container, not individual iterations. this simplifies dependency management while ensuring all iterations complete before downstream processing begins.

- **Use Case Examples:**
    * **Geographic Processing:** process data for each state/region in parallel
    * **Time Period Processing:** handle different date ranges with the same logic
    * **Customer Segment Processing:** apply the same analysis to different customer segments
    * **File Processing:** process multiple files with identical logic

- The container concept is crucial for understanding for each task behavior;

- container management: the for each task act as a single logical unit in your workflow,even it executes multiple iterations internally.

- dependency simplification : downstream task only need to depend on the for each container, not each individual iteration, keeping workflow diagram clean and manageable.

- resource management: the container mananges resource allocation across iteration. optimizing cluster utilization and preventing resource conflicts.

<div align="center">
  <img src="Images/Screenshot%202026-06-17%20at%205.51.12 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- Implementing For Each tasks requires understanding two distinct components:

    * **The For Each Container:** This top-level task manages the iteration logic, input array processing, concurrency settings, and resource allocation. It defines how many iterations run in parallel and how the input array is processed.

    * **The Nested Task:** This is the actual work that gets performed for each iteration. It can be any task type - notebook, SQL, Python script, etc. The nested task receives each array item as `{{input}}` and processes it according to your business logic.

    * **Configuration Flexibility:** This separation allows you to configure iteration behavior independently from the processing logic, making For Each tasks both powerful and maintainable.

---

## Handling Task Failures and Monitoring Jobs Performance

### Overview

This section covers how to **recover from task failures** and **monitor Lakeflow Jobs performance**. Key tools include:
- **Repair and Rerun** — for efficient recovery from failed job runs
- **System Tables** (`system.lakeflow`) — for cost and performance tracking
- **Spark UI** — for identifying bottlenecks at the execution level

### Learning Objectives

By the end of this section, you will be able to:
- Utilize the **repair run** feature to efficiently recover from failed job runs
- Use **system tables** (`system.lakeflow`) and **Spark UI** to monitor performance, track SLAs, identify bottlenecks, and manage resource costs

---

### A. Handling Task Failures

#### A1. Repair and Rerun

> Failure handling isn't just about restarting tasks — it's about building **resilient systems** that can recover efficiently and maintain **data consistency** even when components fail.

**Key Concepts:**
- When a job run fails, instead of re-running the entire job from scratch, **Repair and Rerun** allows you to re-execute only the **failed and skipped tasks**
- This saves compute cost and time — successfully completed tasks are **not re-executed**
- Useful in long multi-task pipelines where only one task fails due to a transient error

**How to use Repair Run:**
1. Go to the failed job run in the **Databricks Jobs UI**
2. Click **Repair Run**
3. Select the failed tasks to re-run
4. Optionally update task parameters before re-running
5. Click **Repair** — only the selected failed tasks execute again

**Benefits:**
- Preserves already-processed data — avoids duplicate writes
- Reduces recovery time and cost
- Maintains job run history for full auditability

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%202.04.00 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

**Repair Feature — Key Points:**

- **Targeted Recovery** — re-run only failed tasks, not the whole job → saves time and compute
- **Parameter Override** — you can change task parameters during repair (fix config, adjust resources, update logic) without rebuilding the job
- **Recovery Scenarios:**
    - fix wrong parameter values that caused failure
    - increase memory/compute for resource-failed tasks
    - deploy code fixes and re-run only affected tasks
    - adjust logic to handle bad data found during run
- **Cost Efficient** — only failed tasks consume compute → reduces unnecessary cost in large pipelines


**A2. Repair Run:**
- click the highlighted boxes to learn about fixing a task.

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%203.23.58 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- allowing you to **run only failed tasks** saving you time and money by only re runing the necessary tasks instead of entire job

**Selective Re-execution — Benefits:**

- **Resource Optimization** — running only failed tasks can reduce recovery time by **80-90%** in complex pipelines → saves time and cost
- **Reduced Risk** — smaller recovery = less load on system resources → lower chance of cascading failures during recovery
- **Faster Resolution** — teams don't wait for full workflow to complete → quicker response to failures → better SLA adherence

> **Important:** Fixing a task in Repair Run does **not** fix the job itself.
> Example: If you passed a wrong parameter and fixed it via repair run, you still need to **manually update that parameter in the job definition** to prevent the same failure next time.

**A3.After Repair Run**

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%203.30.58 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- after re running task your final job will look like this.

**After Repair Run — Key Observations:**

- **Audit Trail** — full visibility into what was repaired, when, and by whom → useful for troubleshooting and process improvement
- **Success Validation** — clearly shows which tasks were recovered successfully → gives confidence in the repair process
- **Learning Opportunities** — historical repair data helps teams spot failure patterns → improve initial job design to prevent future issues

### B. Monitoring Jobs Performance

-  Failure handling is not just about restarting tasks  - its about building resilient system that can recover efficiently and maintain data consistency even when component fail.

**System tables:**

-  system lakeflow is a built in read only catalog that logs all job activity across workspaces in the region.

- Time table : slice long runs hourly using periods_start_time and period_end_time, enabling reliable duration, concurrency, and SLA analytics.

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%203.36.38 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


**Spark UI**

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%203.37.16 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

**Timeline**

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%203.37.45 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- **timeline** in ob run highlights task start/end duration and overlap to spot bottleneck fast.

- click a task to see status, timestamps,duration, cluster/runtime,logs, and quick I/O

- click on query/code details for full text, run ID, wall-clock split, files read/written details,files & partitions and spills details.

- **High plannig time**
    * improve pruning/partitioning
- **High execution time**
    * optimize joins/aggregations(broadcast,skew fixes)

---

### system.lakeflow — Enterprise Monitoring

- **Comprehensive Logging** — all job activity across all workspaces in the region is auto-logged → complete visibility into execution patterns and trends
- **Timeline Analysis** — uses `period_start_time` and `period_end_time` to slice long jobs into hourly segments → accurate duration, concurrency, and SLA tracking

**Key Tables:**

| Table | Purpose |
|---|---|
| `jobs` | Basic job metadata and config |
| `job_tasks` | Task definitions and config details |
| `job_run_timeline` | Full execution history for every job run |
| `job_task_run_timeline` | Detailed execution history per task |
| `pipelines` | Info about Delta Live Tables pipelines |

- **Analytics** — enables cost analysis, performance trending, SLA compliance tracking, and resource utilization optimization

---

### Spark UI — Performance Insights

- **Timeline Analysis** — highlights task duration, overlap, and bottlenecks → quick identification of performance issues
- **Task-Level Details** — click any task to see: execution timestamps, resource usage, cluster config, logs, I/O stats
- **Query Performance Details** — shows execution plans, optimization decisions, file access patterns, partition info, and data spill details

**Actionable Insights:**

| Issue | What it means | Fix |
|---|---|---|
| **High Planning Time** | Poor partitioning / metadata overhead | Better partitioning strategies, metadata optimization |
| **High Execution Time** | Slow joins or aggregations | Broadcast joins, skew handling, query optimization |
| **Resource Bottleneck** | Memory / CPU / I/O constraint | Adjust cluster config (more memory, bigger instance) |


## Lakeflow Jobs in Production and Best Practices

### Overview
This section covers how to run **Lakeflow Jobs in production** using appropriate compute, pricing, modular design, Git integration, and operational best practices.

### Learning Objectives

By the end of this section, you will be able to:
- Select appropriate compute (**serverless vs. classic**) and understand the Jobs pricing structure
- Apply **modular orchestration** design patterns using the Run Job task
- Configure **Git integration** for version-controlled job definitions
- Apply production best practices: **service principals, parameterized tasks, alerting, and maintainable design**

---

## A. Common Best Practices

> Moving from development to production requires understanding how to **design, deploy, and operate** Lakeflow Jobs at enterprise scale — with appropriate governance, security, and operational practices.

**Compute** 
- selecting a right compute options of performance, cost, and operational requirements.

**Modular desing**

- Implementing architecture patterns that support maintainability, reusability and team collaboration.

**Git**

- version control and deployment practices that ensure consistency and enable CI/CD workflow

**Performance monitoring**

- proactive monitoring and optimization strategies that ensure SLA compliance and cost efficiency.

### A1.selecting compute

**Interactive cluster**
- best of performing ad-hoc analysis, data exploration, or development, but not production 
- costly for job runs
- limited scalability 
- availability could be an issue because of parallel usage

**Job cluster**
- cheaper as they terminate when the job ends, reducing resource usages and costs.
- start-up latency
- subject to cloud provider start up time
- maintenance burden

**Serverless compute** represents the optimal choice for most production workloads because it provides operational simplicity, performance optimization, reliability and speed, and cloud independence.

### A2. Pricing structure
- The traditional pricing model involves multiple cost components whereas serverless fundamentally simplifies the cost model.


**Classic pricing**

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%204.27.40 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- The traditional pricing model involves multiple cost components:

**Direct Costs**: DBUs paid to Databricks plus infrastructure costs paid directly to cloud providers (VMs, networking, security services).
**Operational Overhead:** Often overlooked but significant costs including time spent on infrastructure deployment, automation development, maintenance activities, cost monitoring, and efficiency optimization.
**Hidden Complexity:** Managing multiple billing relationships, optimizing across different cost categories, and maintaining expertise in cloud infrastructure management.
- Serverless fundamentally simplifies the cost model:
**Unified Billing:** Single DBU price that includes infrastructure and operational costs, eliminating the need to manage multiple vendor relationships and cost optimization strategies.
**Value Proposition:** The fully managed service provides operational simplicity and reliability improvements that often justify higher per-unit costs through reduced operational overhead.
**Performance Benefits:** Auto-scaling capabilities and out-of-the-box optimizations often deliver better performance at lower total cost than self-managed alternatives.
**TCO Advantages:** When you factor in operational overhead, the total cost of ownership is typically lower with serverless, especially for organizations without dedicated platform engineering teams.

**serverless pricing**

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%204.28.04 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

## B.Modular design

- Implementing architecture patterns that support maintainability, reusability, and team collaboration.

### Modular design in databricks lakeflow jobs

- Implementing architecture patterns that support maintainability, reusability, and team collaboration.

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%204.33.28 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


- Modular orchestration transforms large, monolithic workflows into maintainable, reusable components:

**Decomposition Strategy:** Break complex DAGs into logical business units rather than technical components. Each module should represent a cohesive business function that can be developed, tested, and deployed independently.
**Parent-Child Relationships:** Parent jobs orchestrate child jobs, creating clean separation of concerns while maintaining overall workflow coordination.
**Benefits Realization:**
   - **Maintainability:** Smaller jobs are easier to understand, modify, and troubleshoot
   - **Reusability:** Child jobs can be reused across multiple parent workflows
   - **Team Collaboration:** Different teams can own different modules while collaborating on the overall workflow
   - **Testing:** Individual modules can be tested independently, improving quality and reducing deployment risk

## C. Jobs and Git

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%204.37.52 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- jobs support execution of the notebooks from git

**Change Management:** Prevents unintentional changes to production jobs by ensuring all modifications go through proper version control processes.
**Single Source of Truth:** Eliminates confusion about which version of code is running in production by always executing from committed code in specific branches or tags.
**CI/CD Integration:** Enables automated testing and deployment pipelines that can validate changes before they reach production environments.
**Platform Support:** Broad compatibility with GitHub, GitLab, AWS CodeCommit, and other Git providers ensures you can integrate with existing development workflows regardless of your chosen platform.
**Collaboration Benefits:** Multiple developers can collaborate on workflow development using standard Git workflows (branches, pull requests, code reviews) while maintaining production stability.


### C1. Jobs and Git Configuration Steps

1. **Create task with Git provider as Source**
- Create tasks with Git provider as the source, specifying the repository, branch/tag, and authentication credentials.

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%204.40.37 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

2. **Configure path to main notebook under repository root**

- configure the path to your main notebook under the repository root, ensuring the job can locate and execute the correct entry point for your workflow.

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%204.42.03 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

## D. Best Practices

**compute & cost optimization**

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%204.43.16 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- focus: efficient production compute with lower startup overhead and cost.

**orchestration and modularity**

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%204.46.01 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

**Monitoring and governance**

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%204.46.19 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

---

# 2. Build Data Pipelines with Apache Spark Declarative Pipelines

## Lecture - Introduction to Data Engineering in Databricks

**overview**

- This lecture introduces how Databricks supports efficient data engineering by combining optimized storage techniques, unified data governance through Unity Catalog, and Lakeflow's integrated capabilities for data ingestion, transformation, and orchestration. It then focuses on Apache Spark™ Declarative Pipelines, a low-code, declarative framework for building, managing, and automating reliable batch and streaming data pipelines using SQL or Python.

**Learning Objectives**

- By the end of this lecture, you will be able to:

* **Understand** the components of Data Engineering with Lakeflow in Databricks with Connect, Spark Declarative Pipelines, and Jobs
* **Explain** how Apache Spark™ Declarative Pipelines incrementally processes data using Streaming Tables and Materialized Views in batch or streaming Jobs


### A. Data Engineering Platform Overview
- It all begins with optimized storage using Delta Lake, Parquet, or Iceberg, built upon by unified governance with Unity Catalog, and powered by Lakeflow to deliver end-to-end, high-quality data engineering for analytics and AI.

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%205.12.43 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


- It all begins with optimized storage using Delta Lake, Parquet, or Iceberg.
Built on top of this storage layer is unified governance with Unity Catalog. Unity Catalog is a centralized data catalog that provides access control, auditing, data lineage, quality monitoring, and data discovery across Databricks workspaces.

- Databricks then offers Lakeflow, an end-to-end data engineering solution that empowers data engineers, software developers, SQL developers, analysts, and data scientists to deliver high-quality data for downstream analytics, AI, and operational applications. Lakeflow provides a unified platform for data ingestion, transformation, and orchestration, and includes the following components:

**Lakeflow Connect:** A set of efficient ingestion connectors that simplify data ingestion from popular enterprise applications, databases, cloud storage, message buses, and local files.

**Apache Spark™ Declarative Pipelines:** A framework for building batch and streaming data pipelines using SQL and Python, designed to accelerate ETL development.

**Lakeflow Jobs:** A workflow automation tool for Databricks that orchestrates data processing tasks and workflows. It enables coordination of multiple tasks within complex workflows, allowing for the scheduling, optimization, and management of repeatable processes


### B. Apache Spark™ Declarative Pipelines


<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%205.15.10 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


### C. It’s Difficult to Build and Operate Reliable Data Pipelines


**C1. Challenges in Building Reliable Data Pipelines**

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%205.16.11 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


- Your data engineers just want to focus on transforming and aggregating data—getting it into the right shape for the business.
  *  But first, they have to figure out where the data lives—spread across both the data lake and the data warehouse.
   * Then, they need to make it work for BI reporting, data science, and machine learning.
   * They’re supporting streaming pipelines for new use cases, enabling Generative AI projects, and managing orchestration—all at once.
   * And that’s before they even get to version control, CI/CD, and deployment infrastructure.
   * On top of that, they’re handling data quality checks, governance, and discovery—each with its own complexity.
   * Finally, there’s the operational heavy lifting: hand-coding backfills, managing dependencies, partitions, checkpointing, retries…
- All of this when what you really care about is just delivering reliable data.


**C2. Why Reliable Data Pipelines Are Hard to Build**
- its difficult to build and operate reliable data pipeline for a varity of reasons..

**Labor-intensive development** : Delays data products and insights, slowing business impact.
**Operational Complexity** : Inefficiencies drive downtime, engineering toil, and wasted resources.
**Siloel batch and streaming**:Adapting to evolving latency, cost, and SLA needs is cumbersome and expensive.

### D. Spark Declarative Pipelines: Reliable Data Pipelines Made Easy

- Apache spark declarative pipeline let your team focus on what really matters - writting and managing transformation logic.
    * no more getting bogged down in orchestration, infrastructure, or edges-case operations.
    * just clean, relaible and data pi p eline.
    * all delivered with less effort and more confidence.

**D1. Introducing Declarative Pipelines: Reliable data pipelines made easy**

- with spark declarative pipelines, we are making it dramatically easier to build and manage reliable data pipelines at scale. the following sections walk through how spark declarative pipeline address each of these challenges.

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%205.35.35 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%205.35.59 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

- overall, this is about empowering your tem to move faster with fewer headaches while delivering production-grade data pipeline that just work.

### E. Connecting to Data Sources

- it all start with getting your data into databricks - and thats where lakeflowd connect play a crucail role.

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%205.41.20 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


- It all starts with getting your data into Databricks, and that’s where Lakeflow Connect plays a crucial role.
You can bring in data from a variety of sources. Whether it is stored in cloud object stores such as S3, ADLS, or GCS, streaming from message queues like Kafka, Pub/Sub, or Kinesis, pulled from traditional databases including SQL Server or Postgres, or coming from SaaS applications like Salesforce or Workday, Lakeflow Connect makes ingestion simple and reliable.

- Once the data is connected, Spark Declarative Pipelines can handle ingestion and transformation efficiently. This allows you to build data pipelines that follow the medallion architecture, progressing data through bronze, silver, and gold layers with confidence in reliability and scalability.

- No matter what your data source, you can ingest, transform, and operationalize your data quickly, all within the Databricks environment


### F. Simplifying Batch and Streaming ETL in the Medallion Architecture

<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%205.42.40 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


- With Spark Declarative Pipelines, you can focus your ETL efforts by ingesting data using either batch or streaming method.
 1.**Batch ingestion** is designed for large volumes of data that arrive all at once.
 2. **Streaming ingestion,** on the other hand, handles data that flows continuously in small increments, enabling near real-time processing.
- Once your data is ingested, you’ll follow the medallion architecture to organize and refine it:

3. **Bronze** is the raw data ingestion layer in your lakehouse. Here, data is brought in as-is, sometimes with additional metadata columns added to provide context about the ingestion process.

4. **Silver** is where cleaning and transformation happen, turning raw data into a more usable and refined table.

5. **Gold** represents the final business-level aggregates designed for downstream use cases such as reporting, machine learning, AI, streaming analytics, and more.

- This structured approach helps ensure data quality and usability across your organization’s analytics and operational needs. Spark Declarative Pipelines make this entire process easy to create, monitor, and optimize, so you can focus on driving insights rather than managing complexity.


### G. Incremental Processing in Declarative Pipelines

**G1. Overview - Incremental Processing in Declarative Pipelines**

- lest review at a high how declarative pipline make implementing increamental processing easy. This is the fundamental reason to use them..


<div align="center">
  <img src="Images/Screenshot%202026-07-15%20at%205.45.50 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


- Your data starts out living in some raw data source(s). Whether cloud storage, databases, SaaS applications, or message queues.
- From there, you choose how to ingest that data: batch or streaming. One of the great benefits is the flexibility to switch between these modes easily if your needs change.

- Typically, you’ll build a bronze, silver, and gold ETL pipeline. In this simple example, we’ll create:

   * A bronze streaming table, A silver streaming table and A gold materialized view
- Later in this course, we’ll dive deeper into the fundamentals of streaming tables and materialized views. For now, focus on understanding the overall architecture and how data incrementally flows through these data types in the pipeline.

**G2. Run 1- incremental processing in declarative pipleline**  

<div align="center">
  <img src="Images/Screenshot%202026-07-19%20at%204.40.13 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


- **In the first run** of this pipeline, raw data will be ingested from the data source and appended to the **bronze streaming table**.
- Next, the pipeline transforms the new data from the bronze layer according to your transformation logic and appends the results to the **silver streaming table**.
- Finally, the **materialized view** processes the data at the gold level. Depending on various factors, this processing can happen incrementally or as a full refresh, ensuring your gold-level data is always up to date and ready for downstream use. Materialized views contain a variety of built in optimizations when run on Serverless.

**G3. Run 2 - Incremental Processing in Declarative Pipelines**

 When the pipeline runs a second time, it uses checkpoints to track what data has already been ingested and processed (Auto Loader).In this scenario, let's imagine new data has arrived in the raw data source.
 - In this case, only the new data is incrementally ingested from the data source and appended to the bronze streaming table. The pipeline ignores data it has already processed to avoid processing overhead from the previously ingested data.
 - Next, only the new data from the bronze table is incrementally transformed and appended to the silver streaming table, ignoring data it has already transformed in the first run, again avoiding processing overhead from the previous transformation.
 - Finally, the materialized view processes the full silver streaming table at the gold level. Depending on various factors, this processing can be incremental or a full refresh, ensuring your gold-level data stays accurate and up to date for your downstream consumers.

 This incremental processing approach improves efficiency by only handling new data that hasn’t been processed before. It avoids unnecessary reprocessing of existing data. Materialized views add even more value by providing built-in optimizations, making it faster and more efficient to aggregate or query the streaming tables.


### H. Creating a Spark Declarative Pipeline

There are two easy ways to create a Spark Declarative Pipeline directly from the Databricks workspace.

Both ways will take you to the same pipeline creation flow, where you can start defining your ingestion and transformation logic using SQL or Python.


**method 1: workspace menu**

<div align="center">
  <img src="Images/Screenshot%202026-07-19%20at%204.55.31 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

**method2 : jobs& pipelines**

<div align="center">
  <img src="Images/Screenshot%202026-07-19%20at%204.55.31 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


# 3. Course Project and dataset types overview

## A. course project overview
Throughout this course, we’ll build a hands-on project together using Apache Spark™ Declarative Pipelines. The project begins with files stored in cloud storage. For this course, all files will be in JSON format, but keep in mind that Lakeflow supports many file types in real-world pipelines.

We’ll build three flows within a single pipeline


**step1** 

What this flow does

- Ingests orders JSON files into an orders_bronze streaming table.
- Transforms the data into an orders_silver streaming table.
- Creates the materialized view gold_orders_by_date summarizing the number of orders by date.

<div align="center">
  <img src="Images/Screenshot%202026-07-19%20at%204.59.54 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

**step2**
What this flow does

- Ingests status JSON files into a status_bronze streaming table.
- Transforms the data into a status_silver table.
- Joins the orders and status tables to create the materialized view full_order_info_gold.

Produces two materialized views

1. cancelled_orders
2. delivered_orders

<div align="center">
  <img src="Images/Screenshot%202026-07-19%20at%205.01.50 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

**step3**

What this flow does

- Ingests customer JSON files into a customers_bronze table.
- Cleans the data into a refined bronze table named customers_bronze_clean.
- Performs CDC to track data update changes in the type1_customers_silver table.

<div align="center">
  <img src="Images/Screenshot%202026-07-19%20at%205.02.12 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


## B. Dataset Types

Lakeflow Declarative Pipelines support three main dataset types, each designed for a different type of data processing.

<div align="center">
  <img src="Images/Screenshot%202026-07-19%20at%205.05.49 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

## C. Streaming Table

### C1. Streaming Table Overview


Streaming Tables are designed specifically for streaming or incremental data processing. This means they only process new data as it arrives, rather than reprocessing everything each time the pipeline runs. This approach significantly increases efficiency and decreases cost, especially when working with large or frequently updated data.

<div align="center">
  <img src="Images/Screenshot%202026-07-19%20at%205.05.57 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


**Auto Loader vs Normal Streaming (Short Notes)**

 1. Without Auto Loader (Normal Spark Streaming)

```python
spark.readStream.format("csv").load("/input")
```

**How it works internally:**

* Reads files from the folder.
* Uses **checkpoint** to remember processing progress (so it doesn't reprocess old files).
* Every trigger, Spark **lists the directory again** to discover new files.
* As the number of files grows (e.g., millions), directory listing becomes slower.

**Flow:**

```text
Folder
   │
   ▼
List all files
   │
   ▼
Find new files
   │
   ▼
Process them
```

---

 2. With Auto Loader

```python
spark.readStream.format("cloudFiles").load("/input")
```

**How it works internally:**

* Maintains its own **metadata** of discovered/processed files.
* Uses **cloud notifications** (preferred) or **optimized incremental listing** to discover only new files.
* Avoids repeatedly scanning the entire directory.
* Scales efficiently to millions/billions of files.

**Flow:**

```text
Folder
   │
   ▼
Auto Loader
(Metadata + Efficient File Discovery)
   │
   ▼
Process only new files
```

### C2. Create a Streaming Table with SQL — Bronze Layer

Let’s walk through a basic example of how to create a STREAMING TABLE named orders_bronze from a set of JSON files in cloud storage.

**Streaming table orders_bronze reading from JSON files**

```sql
CREATE OR REFRESH STREAMING TABLE 1_bronze_db.orders_bronze AS
SELECT
*,
current_timestamp() AS processing_time,
_metadata.file_name AS source_file
FROM STREAM read_files(
"{{ source_path }}/orders",
format => 'JSON');
```


In this example, we use the CREATE OR REFRESH STREAMING TABLE statement to create a streaming table called orders_bronze in the 1_bronze_db schema.

The SELECT clause defines which columns you want to pull into the streaming table from your source data.

In the FROM clause:

- The STREAM keyword tells the pipeline to use streaming semantics, meaning it will process new files incrementally as they arrive.
- The read_files() function points to the location of your source files. It reads the files and returns the contents in a tabular format.
This setup ensures that only new data is processed each time the pipeline runs, enabling efficient, scalable ingestion directly from cloud storage.


### C3. Create a Streaming Table with SQL — Silver Layer

Now let’s take it a step further and create a silver streaming table named orders_silver, which reads from the orders_bronze streaming table.


**RESULT: Streaming table orders_silver reading from orders_bronze**

```sql
CREATE OR REFRESH STREAMING TABLE 2_silver_db.orders_silver AS
SELECT
  order_id,
  timestamp(order_timestamp) AS order_timestamp,
  customer_id,
  notifications
FROM STREAM 1_bronze_db.orders_bronze;
```

The SELECT clause is where you apply your SQL transformation logic to the streaming data. In this example, we’re keeping it simple, but in practice these transformations can include complex joins, filters, column derivations, and more.
The most important part is in the FROM clause. Here, you use the STREAM keyword with the name of the source streaming table, in this case, orders_bronze. This tells the pipeline to incrementally transform only the new rows that have landed in the bronze table since the last run.

This setup helps ensure that your silver table always stays up to date with the latest processed data, without reprocessing anything that’s already been handled.


## D. Materialized Views

### D1. Materialized Views Overview

Materialized Views process records as needed to deliver accurate results based on the current state of your streaming tables. They are designed to automatically keep results up to date as new data flows through upstream tables.


<div align="center">
  <img src="Images/Screenshot%202026-07-19%20at%205.39.13 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

### D2. Create a Materialized View with SQL — Gold Layer


Let’s take a look at an example of how to create a Materialized View called gold_orders_by_date, which summarizes data from the orders_silver streaming table.


**RESULT: Materialized view gold_orders_by_date summarizing orders_silver**

```sql
CREATE OR REFRESH MATERIALIZED VIEW 3_gold_db.gold_orders_by_date AS
SELECT
  date(order_timestamp) AS order_date,
  count(*) AS total_daily_orders
FROM 2_silver_db.orders_silver
GROUP BY date(order_timestamp);
```

We use the CREATE OR REFRESH MATERIALIZED VIEW statement to define this view inside the 3_gold_db schema.

In the FROM clause, we reference the source table, orders_silver, without the STREAM keyword. That’s because Materialized Views automatically track changes and manage refreshes based on the upstream streaming table.

Important note: Where possible, the system will use incremental refreshes to update the view efficiently, rather than rebuilding it from scratch. This is supported in Serverless compute and driven by a cost-based optimizer for performance.

This approach is ideal for creating gold-layer aggregations or business-ready outputs with minimal overhead and high performance

## E. Temporary Views and Views

### E1. Views Overview and Limitations

Views create virtual tables that do not store any physical data. Instead, they are simply logical representations based on the SQL query defined in your pipeline.

There are two types: Temporary Views, which exist only for the duration of a pipeline run, and Views, which are registered as objects in Unity Catalog and persist beyond the run


<div align="center">
  <img src="Images/Screenshot%202026-07-19%20at%205.43.37 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


### E2. Create Views with SQL


Both view types use standard SQL CREATE syntax without the OR REFRESH clause. The key difference is scope: a temporary view is discarded after the pipeline run, while a standard view is persisted in Unity Catalog.

**Temporary View**
Pipeline-scoped, not registered in Unity Catalog:

```sql
CREATE TEMPORARY VIEW orders_active AS
SELECT *
FROM 2_silver_db.orders_silver
WHERE notifications = 'Y'; 
```

**View**
Registered in Unity Catalog, available after the pipeline run:

```sql
CREATE VIEW 3_gold_db.orders_active_view AS
SELECT *
FROM 2_silver_db.orders_silver
WHERE notifications = 'Y';
```

## F. What Changed from DLT to SDP

### F1. Syntax Migration Reference

Existing users of Delta Live Tables (DLT) will notice that the syntax has evolved under Spark Declarative Pipelines (SDP). The old keywords remain supported for backward compatibility, but all new pipelines should use the updated syntax.

<div align="center">
  <img src="Images/Screenshot%202026-07-19%20at%205.47.32 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

For existing users of DLT, you’ll notice that we’ve updated some names in Apache Spark™ Declarative Pipelines.

The core semantics and functionality remain the same, so your current workflows will continue to work seamlessly.

We also support the old syntax to maintain backward compatibility during the transition.

Our goal with these changes is to simplify the syntax and better align with industry standards, making it easier to learn and use across different systems.


## G. The Declarative Pipeline Graph

### G1 automatic dependecy resolution
One of the powerful features of Declarative Pipelines is that pipeline dependencies are automatically parsed. You don’t need to worry about the order of your code.

All dependencies are connected behind the scenes in the Declarative Pipeline Graph, which ensures data flows correctly from one step to the next.

<div align="center">
  <img src="Images/Screenshot%202026-07-19%20at%205.48.57 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

For example:

The orders_bronze table ingests the raw JSON data.
The orders_silver streaming table is linked to orders_bronze, transforming the ingested data incrementally.
The gold_orders_by_date materialized view depends on orders_silver streaming table, aggregating the transformed data for downstream business use.
This automatic linking simplifies pipeline development and reduces errors by removing the need to manually manage execution order. It also makes it easy to view and monitor your pipeline and its dependencies, giving you clear visibility into how data flows through each stage


# 4. Apache Spark™ Declarative Pipeline Fundamentals

## A. Simplified Pipeline Development

### A1. What Is the Multi-File Editor?

The multi-file editor in Apache Spark™ Declarative Pipelines makes developing and debugging your ETL pipelines easier and more efficient.

Instead of managing one large file, your pipeline is organized as a set of files visible in the pipeline assets browser, letting you edit code and configure your pipeline components all in one place.


### A2. Key Features of the Multi-File Editor

The multi-file editor in Apache Spark™ Declarative Pipelines makes developing and debugging your ETL pipelines easier and more efficient.

Instead of managing one large file, your pipeline is organized as a set of files visible in the pipeline assets browser, letting you edit code and configure your pipeline components all in one place.


## B. Common Pipeline Settings


### B1. Overview — Accessing Pipeline Settings


Declarative Pipelines come with a variety of settings you can easily configure to customize how your pipeline behaves.


<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%2012.10.02 AM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


**Edit Settings Directly in the Editor**
With the new multi-file editor, accessing and editing pipeline settings is simple and intuitive.
- **Quick Access Settings:** Just click the small gear icon in your editor window to quickly open the pipeline settings popup. No need to leave your development environment.

- **Seamless Configuration:** From there, you can adjust key settings and options directly, helping you tailor your pipeline to your specific requirements without interrupting your workflow.

**3 most common settings**

1. compute : resource and enviroment
2. code assets: files and code modules
3. configuration : pipline parameters

### B2. Compute

Let’s begin with Compute, which lets you select the resources and environment where your pipeline will run.


**Serverless Compute**:(DATABRICKS RECOMMENDED)
**Optimized Cost & Performance**
Serverless optimizes costs while maintaining strong performance.
**Focus on Code, Not Infrastructure**
It allows you to focus fully on your code without needing to manage or provision infrastructure.
**Incremental Refresh for MVs**
Support for incremental refresh of materialized views.
**Cost-Based Optimizer**
A cost-based optimizer that enables fast and efficient transformations of materialized views, improving pipeline efficiency and speed.
**Performance Optimized Setting**: For time-sensitive Lakeflow Jobs, there's an optional Serverless Performance Optimized setting to boost responsiveness.


**classic(fixed size)**

- **fixed size cluster**:Classic Compute uses fixed-size clusters. Users need appropriate permissions to create compute resources for Declarative Pipelines.

- **Permission & Policy Control**:Workspace administrators can set up cluster policies to control and provide access to compute resources for users.

- **Enhanced Autoscaling — Enabled by Default**:Enhanced autoscaling is enabled by default for all new pipelines using Classic Compute. It automatically adjusts cluster size based on workload volume, optimizing resource utilization and helping control costs without manual intervention.

### B3. Code Assets
Code Assets manage the files and code modules that make up your pipeline. .

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%2012.17.03 AM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

**pipeline root folder** : The Pipeline Root Folder is set to automatically and includes all relevant files within that folder for your pipeline project (unless otherwise specified by the user).

This can be a Git folder, enabling easy version control and collaboration.

**source code section**:The Source Code Section lets you specify which subfolders or individual files to include in your pipeline, typically sub folders and files within the root folder.

These can be:
Python scripts | SQL files | Notebooks | etc.


---

Code Assets setting controls what code files your pipeline uses during execution.
Together, these settings ensure your pipeline runs with the exact code it needs, organized and versioned as part of your project.

---

### B4. Configuration (Parameters)

A pipeline's configuration is a map of key-value pairs that can be used to parameterize your code.

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%2012.40.54 AM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


### B5. Additional Pipeline Settings

Beyond the core settings we've discussed, there are several other pipeline settings you’ll encounter throughout the course.


Other additional features include

**Budget** - Defining cost controls and limits
**Advanced Settings** - Additional options for fine-tuning your pipeline

# 5. Ensure Data Quality with Expectations


## A. What Are Expectations

Apache Spark™ Declarative Pipelines lets you apply data quality rules called expectations during your ETL processes.

These rules validate your data row-by-row to ensure integrity.


**general syntax**

```sql
CONSTRAINT constraint_name
EXPECT (column_condition)
[ON VIOLATION action]
```

## B. The Three Violation Actions

**WARN — Log Violations, Keep Rows**

Default behavior when no action is specified

```sql
CONSTRAINT valid_notification
EXPECT (notifications IN ('Y','N'))
```

**result**

- Invalid rows are still written to the target.
- Logs include counts of valid vs. invalid records and other metrics.

**DROP — Exclude Invalid Rows**
Remove bad records from the target table

```sql
CONSTRAINT valid_date
EXPECT (order_timestamp > "2021-01-01")
ON VIOLATION DROP ROW

```

**Result**

- Invalid rows are dropped from the table.
- The count of dropped rows is logged alongside other metrics


**FAIL — Stop the Flow on Violations**

Fail a specific flow when constraints are broken

```sql
    CONSTRAINT valid_id
EXPECT (customer_id IS NOT NULL)
ON VIOLATION FAIL UPDATE
```

**result**

- Causes a failure of a single flow and does not cause other flows in your 
- pipeline to fail.
- Manual intervention is required to resolve the issue.

## C. Adding Expectations — Full SQL Example

Let's look at an example of how to add constraints in Declarative Pipelines. Here we will create a streaming table named orders_silver with all three constraint types applied.


```sql
-- Define the Streaming Table
CREATE OR REFRESH STREAMING TABLE 2_silver_db.orders_silver
 ( 
    -- WARN Constraint — valid_notifications
   CONSTRAINT valid_notifications EXPECT (notifications IN ('Y','N')),
   --FAIL Constraint — valid_date
   CONSTRAINT valid_date EXPECT (order_timestamp > "2021-01-01") ON VIOLATION FAIL UPDATE,
   -- DROP Constraint — valid_id
   CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
 )
AS
SELECT
  order_id,
  timestamp(order_timestamp) AS order_timestamp,
  customer_id,
  notifications
FROM STREAM 1_bronze_db.orders_bronze;
```


**D. Actions Overview — How Expectations Work in the Pipeline**

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%2012.54.53 AM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

Now let’s look at how expectations are applied within the pipeline architecture.

Here’s what happens step by step:

1. A row of data enters the pipeline.

2. That row is evaluated against any defined expectations (constraints).

- If the row passes all expectations, it’s kept, and the pipeline continues processing as normal.

- If the row fails an expectation, the action defined in the constraint determines what happens next:

  - WARN (default): The failure is logged, the row is still included, and the pipeline continues.
  - DROP: The row is discarded, but the pipeline continues processing remaining data.
  - FAIL: The pipeline fails immediately for that specific flow, and manual intervention is required. Other flows are unaffected.
As of Q2 2025, any materialized views that use expectations will always be fully refreshed during pipeline runs.


# 6. Lecture - Streaming Joins and Deploying Pipelines to Production

## A. What Are Streaming Joins?


When performing joins with streaming tables in Declarative Pipelines, it is important to understand the different join types and how they behave.

**Join Types Covered**


**Stream-Snapshot Join Overview**

Let’s start with the most straightforward: Joining a Streaming Table to a Static Table (sometimes called a Stream-Snapshot Join or Stream-static join depending on the scenario)

In this pattern, we incrementally join new data from a streaming table to a static lookup table to create another streaming table.

For example, imagine you’re ingesting a stream of transactions that includes a country_code column. You can join that with a static reference table that maps each code to a full country name.

This is useful when you want to enrich your streaming data with reference information that doesn’t change often.

**Joining Streaming Tables with a Materialized View**

Another supported pattern is joining two streaming tables using a materialized view.

The goal here is to join all rows from both streaming tables each time the pipeline is run.

This type of join is typically used when both input datasets are changing continuously, and you need to combine them regularly to produce a unified, up-to-date result.

For example, imagine one stream contains customer activity and another contains product catalog updates. You could join them using a materialized view to enrich activity data with the most recent product information.

Because both sides are streaming, a materialized view is required to handle this join efficiently and keep the results current.

The materialized view will process all new rows from both tables and incrementally refresh, depending on pipeline configuration and compute mode.

**Stream-Stream Joins**

The final type of streaming join is a stream-stream join, which is designed to incrementally join new data from two streaming tables as it arrives.

In this pattern, only the new incoming data from each stream is joined, past data is not considered during each run.

These joins are useful for detecting relationships between events that occur close together in time, such as joining clickstream data with real-time ad impressions.

However, because stream-stream joins often involve windowing logic, watermarking, and other advanced streaming concepts, they are outside the scope of this course.

We recommend reviewing additional Databricks documentation or advanced streaming resources if your use case requires this type of real-time event correlation.


## B. Stream-Snapshot Join Overview


As new data arrives in your streaming table, it is joined in real time with the static reference table.

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%2012.54.53 AM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


That means every new record in the streaming table is matched against all available data in the static table, ensuring a complete and accurate join each time.

The result of that join is then appended to your final streaming output table, continuing the pipeline flow.

This is a great pattern for enriching streaming data with context, like converting country codes, product IDs, or user roles into readable formats using static reference data.

It’s efficient, reliable, and works well since only one side of the join, the streaming table, is changing over time.

A key detail to understand here is that as new data is appended to your source streaming table, only the new rows are joined with the entire static lookup table.

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%2012.54.53 AM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


This is an incremental join, meaning the static table doesn’t need to be reprocessed, only the incoming streaming data is evaluated.

This keeps the process highly efficient, and ensures that each new record is immediately enriched with the latest reference data from the static table.

## C. Joining Streaming Tables with a Materialized View

As new data is appended to both streaming tables, the materialized view detects the changes and responds accordingly.

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%2012.54.53 AM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


Each time the pipeline runs, the materialized view will efficiently compute the join by processing all the current data from both streaming tables.

This ensures that every new record from either side is matched correctly, producing an up-to-date and complete output.

The use of a materialized view allows the join to scale efficiently, leveraging incremental refresh where possible to avoid unnecessary recomputation.

This join pattern is ideal when both data sources are live and frequently updated, and you want to keep your results synchronized across both.

As new data is added to the streaming tables, the materialized view will again efficiently compute all the data in the streaming tables and joins the rows.

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%2012.54.53 AM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


## D. Stream-Stream Joins

The final type of streaming join is a stream-stream join, which is designed to incrementally join new data from two streaming tables as it arrives.

- In this pattern, only the new incoming data from each stream is joined, past data is not considered during each run.

- These joins are useful for detecting relationships between events that occur close together in time, such as joining clickstream data with real-time ad impressions.

However, because stream-stream joins often involve windowing logic, watermarking, and other advanced streaming concepts, they are outside the scope of this course.

We recommend reviewing additional Databricks documentation or advanced streaming resources if your use case requires this type of real-time event correlation.


## E. Comparing the Join Types

The table below summarizes all three join patterns at a glance. Use this as a quick reference when deciding which join type fits your pipeline's requirements.

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%2012.54.53 AM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


## F. The Complete Pipeline — What We Have Built

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%2012.54.53 AM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

So far, we’ve completed the first flow of our pipeline:

Orders Flow: orders JSON files → orders_bronze → orders_silver → gold_orders_by_date

Now we’re ready to add the second flow to the pipeline, the Status flow.

Here’s what this new flow will do:

- Ingest status JSON files to the status_bronze streaming table
- Transform the bronze streaming table into the status_silver streaming table
- Join status_silver with orders_silver to create a new materialized view: full_order_info_gold
- From there, we’ll build two additional materialized views:
  - cancelled_orders
  - Delivered_orders
This flow gives us rich insight into order lifecycle states, and demonstrates how we can join flows together and branch logic for more advanced analytics.

After we complete the Status Flow, our next step is to move the pipeline from development to production.


## G. Schedule, Notifications, and Monitoring

Use these key steps to operationalize the pipeline. When you're ready to move your Apache Spark™ Declarative Pipeline into production, there are four key operational tasks to ensure it's reliable, automated, and monitored:

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%2012.54.53 AM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

## H. Scheduling the Pipeline


### Continues Mode

**How It Works**
Pipelines continuously process new data to keep streaming tables and materialized views up to date in near real-time.
**What Happens**
The system monitors dependencies and updates only when source data changes — ensuring efficiency without unnecessary reprocessing.
**Best For**
Streaming use cases where freshness and responsiveness are critical. The pipeline stays alive and processes data as it arrives — keeping results current at all times.

### Trigger mode


**How It Works**Pipelines can be triggered manually or set to run on a recurring schedule (e.g., every 15 minutes, hourly, daily).

**What Happens**
The pipeline refreshes selected tables using the data available at the start of execution. Once updates are complete, the pipeline stops.
**Best For**

Batch processing or pipelines that do not need to run constantly. The pipeline runs, completes its work, and stops — making it cost-efficient for scheduled, non-continuous workloads.

## I. Email Notifications

As part of scheduling your pipeline, you can configure email notifications to keep stakeholders informed of pipeline activity.

Notifications can be set for any combination of three events.

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%201.33.24 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

Configure Notifications for Any Combination of These Events

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%201.35.57 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

## J. Monitoring with the Pipeline Event Log

Another essential task when running pipleline in prodution in production is monitoring their heath and performance.


<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%201.37.37 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

### J1. pipeline event log

spark declarative pipelines provide a pipeline event log that captures all critical information, including:

- **audit log** : tracks who did what and when within the pipeline . provides a full history of actions taken against your pipeline and its datasets.

- **data qualitiy checks**:

monitor the results of any expectations or containts applied during processing including pass/fail counts and voilation details.


- **pipeline progress**

see the status and progress of each pipeline run - including row counts processed, stages completed ,and count execution state.

- **data lineage** 

understand how data flows and transforms through your pipeline from source ingestion through bronze,silver and gold layers.


This comprehensive event log helps you quickly diagnose issues, ensure data integrity, and maintain full visibility into your pipeline operations.


### J2. Querying the Declarative Pipeline Event Log

**publish to metastore(recommended):**

Publish the event log as a Delta table using the advanced settings

- specify the table location(catalog ,schema)

- define the table name


**default behavior**

By default, the event log is written as a hidden Delta table located in the pipeline's default catalog and schema.


**Querying the Event Log**

Once published, you can query the event log just like any other Delta table, enabling you to build custom reports, dashboards, or perform deeper analysis on pipeline activity and health.

```sql
SELECT * FROM <catalog>.<schema>.<event_log_table_name>
```

---

# 7. Lecture - Change Data Capture (CDC) Overview


## A. What Is Change Data Capture?

Change Data Capture (CDC) is a technique used to track and capture changes in a data source (such as a database, lakehouse or data warehouse). Those changes are then applied to a target table, for example, your Lakehouse, to keep it up to date with the latest state from the source.

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%201.48.54 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


CDC is also closely related to how we handle Slowly Changing Dimensions, or SCDs, which define how historical changes are tracked and stored in your target. There are two main types of SCD we’ll focus on:

- SCD Type 1 – Overwrites existing data (no history tracking)
- SCD Type 2 – Tracks historical changes by storing previous versions of records
Let’s walk through a high-level example to make this concrete.

Imagine we’re working with a customer table.

- Our source data contains new customer records, as well as updates and deletes to existing customers.
- We want to apply those changes to our target table using either SCD Type 1 or SCD Type 2 logic (We’ll dive deeper into what those types mean and how they’re implemented shortly) to keep our target customers table up to date with the latest information.


## B. SCD Type 1 — Overwrite Target with Latest Values

### B1. SCD Type 1 — Overview

Let’s start with an overview of Slowly Changing Dimension Type 1, or SCD Type 1. In SCD Type 1, target table is overwritten with the latest values.

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%201.49.00 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


### B2. Worked Example — Step by Step


**our scenario:**


We have a customers table as our target. The table currently contains two customers:
- customer_id 1, Peter
- customer_id 2, Samarth

We have an updates table as our source, this contains:
- Updates (Peter has had two update on his address. One on 5/15 and the other on 5/20, customer_id 1)
- Deletes (Samarth wants to be removed, customer_id 2)
- Inserts (New customer Kostas, customer_id 3)

Our goal is to update the customers table with the new customer information from the updates source table.


<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%201.49.06 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


When we apply SCD Type 1, our target table is updated with the latest customer information, without keeping any historical versions of the data based on the CustomerID (ID column) and ProcessDate (sequence column).


<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%201.49.11 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

Let’s walk through what happens in this example:

- Peter (customer_id 1): His address is updated to the latest address based on the ProcessDate to 123 Main St. from the updates table.
- Samarth (customer_id 2): He’s been deleted, so his row is removed from the target table.
- Kostas (customer_id 3): He’s a new customer, so his record is inserted into the table.
The end result? The customers table contains a current snapshot of all active customers, with no history, just the most recent customers.

This is the simplest CDC strategy, and it’s ideal when maintaining historical changes isn’t necessary. You just need the latest, most accurate data.

## C. SCD Type 2 — Historical Tracking/Versioning

### C1. SCD type2 - Overview


Let’s talk about Slowly Changing Dimensions Type 2, or SCD Type 2, which introduces historical tracking and versioning of records.

**on update or insert**

the old record is preserverd with an additional column indicating its validity period(start date,end date or a current flag). a new row is inserted with the updated information

**on delete**

when a record is marked as deleted, the record is kept and  a column indicating the record is inactive.

**when to use SCD type 2**

used when historical data is important and the system need to track how attribute changes over time.

### C2. Worked Example — Step by Step


**Example of SCD Type 2 in action:**
In the example table, we’ve added two important metadata columns to the target table:
- __START_AT – shows when the row became active
- __END_AT – shows when the row became inactive (if it has)
  - A null __END_AT means the row is currently active.

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%201.49.24 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


lets break down the example above:

- **customer ID 1-peter**

1. the active record shows peters updated address.
- __END_AT is null -> still active.
- __START_AT marks when the update occurred

2. the inactive record holds peters. we can tell it is inactive becuase __END_AT is populated -> longer current

 
- **customer ID2 - samarth**

- since samarth deleted his account, his exiting record is now inactive.
- a date is added to __END_AT to indicated when he was removed.


- **customer ID3 - Kostas**

 a new customer, so his record was inserted into the table.
 
- __START_AT shows when he joined
- __END_AT is null -> the record is currently active.

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%201.49.24 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


## E. The Complete Pipeline — Customers Flow Added

Let's look at the final CDC flow we are adding to our pipeline.

**Customers Flow Overview:**

**Ingest customer JSON files** - Bring raw data into the pipeline from cloud storage into the customers_bronze streaming table.

**Create customers_bronze_clean table** - A cleaned streaming table that filters and formats incremental updates, inserts, and deletes from the customers_bronze streaming table.

**Use AUTO CDC INTO customers_silver**

- Uses SCD Type 1 to overwrite customer changes (updates, inserts, and deletes).
- Implemented using AUTO CDC INTO with STORED AS SCD TYPE 1.
This final flow combines CDC logic with the medallion architecture to maintain an updated customers (with no historical information), critical for downstream analytics, compliance, and personalization


<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%201.49.40 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


---


# 8. Lecture - Introduction to Software Engineering (SWE) Best Practices


## A. Introduction to SWE Best Practices

to build reliable data pipeline, we can learn from software engineering best practices.

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%206.18.58 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

following best practices in development ensure that your data pipelines are efficient, scalable, reliable, and easy to maintain.

In this lesson, we’ll introduce some key software development best practices and explore how they can be applied to building reliable data pipelines.

Incorporating these best practices in data pipeline development helps ensure your pipelines are efficient, scalable, and easy to maintain.

Let’s take a high-level look at some of the most important best practices.


## B. Best Practices


### 1. **Coding Practices**

- We will start with some key coding practices that promote better code development.
- First, **Code Readability**. Write code that’s easy to understand and maintain. Clear, readable code reduces confusion and minimizes the chances of errors when updates or changes are needed.
- Next, **utilizing consistent naming conventions**. Using descriptive, consistent names for variables, functions, and classes makes your code self-explanatory and enhances collaboration among team members.
- Finally, **incorporating modular design** in your code base. This means breaking your project down into smaller, reusable components (like functions). This not only makes your code easier to maintain but also allows for smoother scaling as your project grows.
- You can also use code linting tools to help enforce these practices. Linting tools automatically analyze your code for potential errors, inconsistencies, and style violations. While linting tools are outside the scope of this course, they are an excellent resource for improving code quality and maintaining readability.

### 2. **Document Code**

- Another important best practice is documenting your code.
- Good documentation improves the following:
- First the ability to maintain your code. Clear docs help developers understand the purpose and functionality of the code, making updates and bug fixes quicker and easier.
- Next good documentation improves collaboration. Well-documented code lets team members get up to speed fast, reducing misunderstandings and errors.
- Lastly documentation improves knowledge transfer. By documenting your code it preserves key info about the code design and structure, ensuring smooth transitions when team members change.

### 3. **Automated Testing**

- Testing is an extremely critical component of software development best practices.
- Writing both unit tests and integration tests is essential for verifying that both individual components and their interactions function correctly.
- A unit test verifies the functionality of a single unit or component of code, typically in isolation to ensure it behaves as expected.
- Integration tests, on the other hand, check how different components or systems work together to ensure they function correctly as a whole.
- We will talk more about these later.


### 1. **Version Control and Code Review**

- Another essential best practice is using version control and code reviews on your project.
- With version control, tools like Git are essential for tracking changes, collaborating with your team, and keeping a history of your codebase. They allow you to roll back changes, manage multiple versions, and avoid conflicts, all while keeping your work organized and secure.
- Next is Code Reviews. When combined with version control, code reviews are an effective way to catch bugs early, improve code quality, and ensure consistency in coding standards. Code reviews foster collaboration, encourage knowledge sharing within the team, and ultimately lead to more maintainable and reliable code.
- In short, use version control to manage your codebase effectively, and conducting code reviews help to improve the quality of your code through collaboration.

### 2. **CI/CD**

- Next is CI/CD, or continuous integration and continuous deployment/delivery.
- At a high level,continuous Integration (CI) is when developers regularly commit code, build, test and release code to a shared repository. The goal is to catch issues early through continuous integration and testing.
- Next is Continuous Deployment (CD). This automates the release of code to production after passing your automated tests (unit and integration tests). The goal is to deliver features and fixes quickly and consistently avoiding errors.
- This course mainly focuses on Continuous Integration within the CI/CD pipeline, with a high level overview of Continuous deployment and delivery.


### 3. **Isolated Environments**

- Lastly, you do not want to be modifying code directly on the production codebase.
- Organizations often use different environments for each stage. A typical setup includes “Development & Stage” and “Production,” but this can vary based your organization's processes.
- Separate environments help isolate changes and ensure thorough testing before deployment, preventing issues from mixing development and production.
- In Databricks, you can isolate environments in a few ways:
You can use multiple Workspaces, one for each environment.
- Or, use a single Workspace with multiple catalogs.
- One major advantage of Databricks is Unity Catalog, which provides built-in features like lineage, security, and monitoring, all without needing third-party tools.
This was a quick high level overview of some key software engineering best practices. There are many more that we do not cover here in this overview.

These practices aim to create high-quality, maintainable software that can evolve over time. The focus is on writing efficient, readable, and defect-free code.

As we move forward, keep in mind how these best practices can help you build better, more efficient data pipelines.


## C. Software Engineering with Databricks


**Tools Overview**

**Databricks workspaces**

Develop code and run unit tests in a Databricks Workspaces or locally using Notebooks or Files (SQL, Python, Scala, etc.).

**databricks git folder**
Utilize Databricks Git folders to provide version control and significantly improve the workflow.
**unity catalog**
Focus on using Unity Catalog within a single Workspace or multiple Workspaces to isolate your environments securely, providing the necessary data access.


**databricks deployment tools**

Get code tested & deployed via CI/CD pipelines using Databricks deployment tools to deploy to your desired environment automatically.


# 9. Lecture - Introduction to Modularizing PySpark Code


## A. Modularizing PySpark Code: Non-Modularized Code (Before)

**Issue**

- **everything is in one block**
making it harder to modify or test specific parts, such as loading data or adding new columns.


- **code duplication**

could occur if the same operation are needed elsewhere in the project.


**Non-modularized code(before)**

```python

# 10. Load data
df = (spark
      .read
      .csv("health.csv",
           header=True,
           inferSchema=True))

##Create column
df = (df
.withColumn("NewColumn",
when(col("Column") == 0, 'Normal')
.otherwise('Unknown')))

```

## B. Modularizing PySpark Code: Modularized Code (After)


**non-modularized (before)**

```python

# 11. Load data
df = (spark
      .read
      .csv("health.csv",
           header=True,
           inferSchema=True))

# 12. Create column
df = (df
.withColumn("NewColumn",
when(col("Column") == 0, 'Normal')
.otherwise('Unknown')))
```

**modularized code(after)**

```python

def load_data(file_path):
    return (spark
            .read
            .csv(file_path,
                 header=True,
                 inferSchema=True))

def add_new_col(df, new, s_col):
return (df
.withColumn(new,
when(col(s_col) == 0, 'Normal')
.otherwise('Unknown')))

```


nstead, you want to focus on modularizing your code.

For instance, the code you wrote to read the CSV file into a Spark DataFrame can be turned into a function, such as def load_data().

Similarly, the function that creates a new column based on an existing one could be refactored into def add_new_col(), or, ideally, a more specific name that clearly reflects what the function is doing (example, def add_age_category() if you're categorizing ages).


## C. Modularized Code Benefits


1. Easier to maintain
2. reuse
3. testing


Let’s talk about some of the key benefits of modularizing your code.

First, functions make maintenance easier. When your code is organized into functions, you can update or change specific parts of the code without affecting the entire script. This makes it way easier to manage and fix issues down the line.

Another big benefit is reusability. Once you’ve written a function like load_data() or add_new_col(), you can reuse it across different projects or scenarios. No need to rewrite the same code over and over again. it saves time and reduces errors.

Lastly, functions improve testability. By isolating specific logic into functions, you can write unit tests for each function individually, ensuring that each part of your code is working as expected. This leads to more reliable and bug-free code.


# 13. Lecture - DevOps Fundamentals

## A. What is DevOps?


<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.03.34 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

faster collaboration between development and operation teams to automate lakeflow jobs and streamline processes.


**key benefits include

1. **faster deployments**
2. **improved collaboration**
3. **enhanced reliability**
4. **better scalability**


## B. Devops lifecycle

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.05.58 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


Let’s dive into the 8 key steps of the DevOps lifecycle, breaking each stage down into simple, easy-to-understand points.

First up is Planning. This is where we define project goals, gather requirements, and make sure the whole team is aligned on what needs to be delivered.

Next, we move on to Coding. Here, developers write the application’s source code, building the features and functionality we need.

After that comes Building. This is where we compile the code into executable files, making sure all dependencies are correctly integrated.

Then, we hit Testing. In this stage, we run automated tests to ensure the code works as expected, catching any bugs before we release. Testing is extremely important within DevOps.

Now it’s time for Release. We want to package the application, ensuring it’s production-ready, and prepare for a controlled rollout.

Once the release is ready, we move to Deploying. This is when we push the application to the production environment and make it available to users.

After deployment, we need to Operate the application. This means monitoring the performance, managing its resources, and quickly addressing any issues that come up.

Lastly, we get to Monitoring. Here, we track the app’s performance, gather feedback, and continuously work on improvements to keep things running smoothly.


## C. DevOps as a Continuous Practice

devops is a process for continously integrating,testing,and deplo
yment your code.

The DevOps lifecycle is all about seamless collaboration between development and operations teams to deliver high-quality software.

Continuously iterating throughout the DevOps lifecycle as the project needs fixes, updates or features

## D. DevOps for Data Engineering and Machine Learning


<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.08.32 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

We can apply DevOps principles to Data Engineering and Machine Learning. Remember, DevOps is all about automating processes, improving collaboration, testing, and speeding up delivery.

DataOps is a subset of DevOps and applies DevOps to data engineering. It automates the management of data pipelines, ensuring smooth, reliable data flows from collection to processing. This means fewer bottlenecks and faster insights.

MLOps is about applying DevOps to machine learning. It streamlines the process of deploying and managing ML models, ensuring that models move from development to production quickly and are monitored for performance

## E. DevOps, DataOps and MLOps

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.09.28 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


DevOps, DataOps and ModelOps are a set of practices, processes, and technologies. Let's compare the three approaches that streamline Lakeflow Jobs in different tech areas.

Let’s break them down.

With DevOps, it's all about bridging the gap between software development and IT operations. The main goal is to automate CI/CD, or continuous integration and deployment, making software deployment faster and more reliable.

With DevOps, we also focus on automated testing and version control to ensure code quality. The aim is to establish smooth, production-grade Lakeflow Jobs and automate orchestration to reduce manual work.

Lastly, system performance monitoring helps catch issues early, keeping everything running smoothly.

Next is DataOps, which is all about Building quality data pipeline processes. It optimizes data processing and centralizes data discovery, management, and governance with Unity Catalog.

DataOps also emphasizes traceable data lineage, so you can track data at every stage. Monitoring data throughout the pipeline ensures that it stays accurate and accessible, while promoting team collaboration to improve data flow and quality.

Lastly, we have ModelOps, which focuses on the lifecycle of machine learning models. It treats model code like software, ensuring it’s versioned, tested, and deployed efficiently.

ModelOps also focuses on managing the model lifecycle and monitoring model performance after deployment to ensure models stay accurate and effective over time. MLOps is outside the scope of this course


## F. DataOps = DevOps for Data Engineering

you want to think about how devops principle and cluture can be applied to your data engineering pipelines.


DataOps is essentially DevOps for data engineering—it applies those same principles of automation, collaboration, and continuous improvement to data Lakeflow Jobs. Just as DevOps streamlines software delivery, DataOps focuses on optimizing the flow, quality, and management of data pipelines.

In the end, you want to think about how DevOps principles and culture can be applied to your Data Engineering pipelines.


# 14. Lecture - The Role of CI/CD in DevOps


## A. Continuous Integration (CI) and Continuous Deployment (CD)


<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.12.24 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

CI/CD is a key subset of DevOps practices that focuses on automating code integration, testing, and delivery pipelines, including DataOps pipelines. Within the DevOps lifecycle, continuous integration (CI) emphasizes planning, development, environment management, and testing of the pipelines.

On the other hand, continuous deployment (CD) focuses on automating release processes, deployment, operation, and monitoring of these pipelines.

## B. CI/CD Overview

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.13.13 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

At a high level, let's talk about CI/CD, a practice that is transforming the way we develop and deliver our data pipelines.

CI/CD stands for Continuous Integration and Continuous Deployment/Delivery.

It’s a process that automates and streamlines key aspects of software development. By automating repetitive tasks, CI/CD helps improve code quality, speed up development cycles, and ensure the reliability of the code being deployed. Using CI/CD, code is deployed to production through an automated process, incorporating a quality control process.

So, what exactly does the CI/CD process involve? Essentially, enables teams to develop and deliver software in short, frequent cycles. This is accomplished through using automated pipelines, which ensure that code changes are integrated, tested, and deployed quickly and efficiently. This allows developers to identify issues early and address them before they become bigger problems.

While CI/CD has been a standard practice in software development for years, it is now becoming increasingly important in data engineering and data science as well. As these fields evolve, the need for automation and quick, reliable delivery of data-driven applications is growing.


## C. Continuous Integration (CI) High Level Overview


CI involved regularly merging code changes from multiple contributors into a central repository and runnig automated test to ensure code quality.


<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.14.38 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


CI involves regularly merging code changes from multiple contributors into a central repository and running automated tests to ensure code quality. Test that fail do not make it into your source code.

How the testing and commits to version control is implemented depends on the branching strategy defined by your organization. Deciding on a branching and testing strategy is extremely important.

Here are four key benefits of Continuous Integration:

First, Early Detection of Issues—by integrating code often, bugs and conflicts are caught early, making them easier to fix.

Second, Faster Development Cycle—frequent integration speeds up the delivery of new features and fixes.

Third, Improved Collaboration and Code Quality—regular integration leads to cleaner, more modular code and better teamwork.

Finally, Automated Testing and Validation—automated tests run with each integration to ensure the code is stable and works with existing features.

These benefits—early issue detection, faster delivery, better collaboration, and automated testing—make Continuous Integration essential for smooth development.


## D. High-level Testing Steps


<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.15.42 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

Within CI/CD there are testing steps you should following within what's called the testing pyramid. The testing pyramid categorizes different tests, unit tests, integration tests and system tests.

The base of the pyramid are unit tests which test individual functions or methods in isolation. Since they are small individual functions, they typically can run quickly, frequently and automatically, ensuring that the functions work as expected.

Unit tests form the foundation because they are inexpensive and provide the broadest coverage. For example, testing if a pyspark method works as expected.

Next is integration tests test the interaction between different components or systems. These are typically slower and more costly than unit tests, but provide greater assurance that components work together correctly. Within Databricks these typically will revolve around using Notebooks, SDP and or Lakeflow Jobs. For example, testing whether a pyspark method and SDP work correctly.

Lastly system tests test the entire application, ensuring that all parts function together in a real-world scenario. These are typically slow, costly, and often run in a production-like environment. For example, for our end to end data pipeline, testing whether the data pipeline works as expected within a Job, creating our desired results.

## E. Continuous Delivery/Deployment (CD) Overview


**Continuous Delivery (CD)**

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.16.37 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

Continuous Delivery is all about automating the process of pushing changes to staging or pre-production environments. This setup allows for seamless updates and provides the flexibility to manually deploy to production whenever needed, ensuring smooth, controlled releases. For example, after the continuous integration steps are complete and testing has occurred we can decide to deploy our data pipeline to production.

**Continuous Deployment (CD)**


<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.17.33 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

Continuous Deployment takes automation a step further. Once a change passes all tests, it’s automatically deployed to production, ensuring that new features or fixes are delivered quickly and seamlessly, without manual intervention.

For example, if your continuous integration processes are aligned and well implemented, we can automatically deploy our data pipeline from development, to staging, then production, avoiding manual deployment steps. Implementing this technique required well thought out tests to ensure the pipeline should be deployed.


## G. High-Level CI/CD Workflow Overview

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.18.06 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


In the end, the CI/CD process streamlines development of your data pipelines by automating testing and deployment, leading to faster, more reliable releases.

This approach minimizes manual errors, improves collaboration, and ensures high-quality software delivered quickly and consistently.


# 15. Lecture - Planning the Project

## A. Requirements

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.19.16 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


This project focuses on visualizing health data by following a structured data pipeline. We begin by ingesting daily incremental CSV files into a Bronze table, then refining this raw data into a clean Silver table. From there, we create a summarized Gold table, which is shared with consumers and used to build the final visualization. Throughout the process, we make use of various database assets, including notebooks, Spark Declarative , Lakeflow Jobs, and compute resources, to ensure a smooth and efficient execution.

## B. Setting Up Your Data Environments

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.20.06 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


Within a CI/CD pipeline

In development, data is often anonymized or generated using synthetic datasets to allow rapid development and testing without compromising privacy or production data integrity.

If you have an staging environment, staging data should closely mirror production in structure and volume, with anonymized or scrubbed sensitive information to ensure realistic testing and validation.

Production data is live, fully operational, and continuously updated, containing real user data, and must be handled with high security, privacy, and compliance standards.

Each environment should have data suited for its specific purpose, balancing realism, security, and compliance at every stage. How this is implemented will depend on your organization and the sensitivity of your data


## C. Isolating Environments

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.21.09 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

Isolating your environments for the different stages of development is key to developing a CI/CD pipeline. This ensures that code is developed and tested in the developing and staging prior to touching the production environment.

The minimal setup is to have two environments “Development & Stage” and “Production”, but this can vary depending on your organizations requirements.


## D. Workspace Isolation Overview

you can isolate your dev, stage and prod enviroments at the workspace and storage level.

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.22.35 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

Within Databricks, one methods for isolating your environments includes creating a separate Workspace for the dev, stage and prod environments. This ensures that all development is isolated from your production environment.

## E. Unity Catalog Isolation

<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.23.22 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


Another method for isolating your environments within Databricks is Unity Catalog isolation.

With this method, you create a catalog for each(dev, stage and prod). The dev environment includes the dev data, stage the staging data, and prod the production data.

With this method you can also utilize Unity Catalog access control for your developers, only giving them the required permissions for each


## F. Course Project Architecture


<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.24.03 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


Now that we have an understanding of our project, our data, and how to isolate environments, let's dive into the project setup.

In this project we will create a catalog for each environment: dev, stage, and prod.

The dev catalog will contain a small, static subset of our production data, used for development and initial testing.

The stage catalog will hold a larger subset of production data, enabling more comprehensive testing as we move through our CI/CD process.

Finally, the prod catalog will contain the live production data that we rely on for final operations.

Our workflow starts by developing on the dev data, running unit and integration tests as we progress through the pipeline. The pipeline is set up with Databricks Lakeflow Jobs, which execute the unit tests, Spark Declarative pipeline, and final visualizations.

As we test the pipeline through each stage, we will ensure everything is functioning correctly before deploying to production.

# 16. Lecture - Introduction to Unit Tests for PySpark


## A. Unit Tests Benefits


<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.43.33 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


Let’s quickly go over the benefits of unit tests:
First, they test a specific function with a small amount of data, which makes it easy to isolate problems.
Unit tests also help you catch bugs early, before deploying to production, saving time and reducing errors.
They make refactoring easier since you can ensure your changes don’t break anything.
And lastly, they make debugging simpler by pinpointing exactly where things go wrong.
In short, unit tests are key to keeping your code reliable and easy to maintain.


## B. Pyspark.testing.utils Testing Functions

pyspark.testing.utils provides helper functions to make unit testing in PySpark easier.


**assertDataFrameEqual**

```md
assertDataFrameEqual(actual, expected[, ...])
```

**assertSchemaEqual**

```md
assertSchemaEqual(actual, expected)
```


There are a variety of other methods to test your unit tests, we will focus on the pyspark testing utils.

PySpark's built-in testing utilities simplify the process, especially when testing Spark transformations and actions.
assertDataFrameEqual is a utility function to check equality between an actual and expected DataFrame, with optional parameters.
assertSchemaEqual is a utility function to check equality between DataFrame schemas actual and expected.
There are a variety of other methods to test your unit tests, we will focus on the pyspark testing utils.


## C. Unit Test Example


**1. You have the following function to create a column**

```python
from pyspark.sql.functions import col, when
def add_new_col(df, new, s_col):
 return (df
         .withColumn(new,                        
            when(col(s_col) == 0, 'Normal')                            
            .otherwise('Unknown')))
```

**2. Your desired results**


<div align="center">
  <img src="Images/Screenshot%202026-07-21%20at%207.47.41 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

Let's take a look at a simple example of a unit test.
You first have the initial function you want to test. In this example it is the add_new_col function.


## D. Unit Test Goal

**3. Create the unit test**


```python
def test_add_new_col():
   data = [(0,), (1,), (-1,),(None,)]
   columns = ["value"]
   df = spark.createDataFrame(data, columns)

actual_df = add_new_col(df, "new_value", "value")

expected_data = [(0, 'Normal'), (1, 'Unknown'),
(-1, 'Unknown'), (None, 'Unknown')]
expected_df = spark.createDataFrame(expected_data,
["value", "new_value"])

assertDataFrameEqual(actual_df, expected_df)

```

A well-implemented CI/CD process enables faster and more reliable releases, reduces errors in production through automated testing, and supports easier rollbacks with safe deployment practices.


## E. Unit Testing Framework - pytest

Pytest is popular testing framework for Python that makes it easy to write simple and scalable test cases.


**Uses Simple Syntax**

Minimal syntax, just define functions starting with test_


**Provides Assertions**

Use assert statements to provide detailed error messages on failure


**Automatic Discovery**
Finds and runs all tests automatically with a simple configuration


**Rich Ecosystem**

Extend functionality with plugins for coverage, parallel tests, and more


# 17. Lecture - Executing Integration Tests with SDP and Jobs


## **A. Executing integration tests**


With Apache Spark™ Declarative Pipelines (SDP) or Lakeflow Jobs

**Spark Declarative Pipelines (SDP):**Use SDP expectations to check pipeline’s results (demo technique).


**Lakeflow Jobs:**Implement it as a Databricks Job with multiple tasks - similarly what is typically done for non-SDP code.

## B. Integration Test Methods


<div align="center">
  <img src="Images/Screenshot%202026-07-23%20at%202.33.22 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

SDP - Method 1 - Expectations:

Let's example the integration test with SDP.

In this example we will use SDP to ingest CSV files from each catalog (dev, stage and prod) based on the pipeline configuration variable, it will simply read the data from the corresponding target environment.

Regardless of the target environment (dev, stage, or prod), the same SDP is executed. Focus on the black squares under the "Shared SDP Code" in the image. The same SDP transformation logic, which includes custom functions, is applied in each environment.

In this example, data from the target catalog is ingested into the "health_bronze" table, then cleaned in the "health_silver" table, and finally aggregated into a materialized view in the "chol_age_agg" gold table for each environment.

The purple boxes beneath the shared SDP represent test materialized views created with SDP expectations during the dev and stage runs. Since we're using static data for dev and stage, we know the expected output, which allows us to test our shared SDP code. In this case, we’re performing simple expectations for demonstration purposed, like counting the number of rows in the tables, to confirm correct ingestion. Typically you would create much more focused tests.

Jobs - Method 2 - Tasks

Instead of using SDP and expectations for integration tests, you can also use Databricks Lakeflow Jobs with tasks.

Looking at the entire Job for this simple project, we start by executing unit tests to test individual functions in isolation. If any unit test fails, the job will fail.

Next, we execute the SDP to ingest the data. In this example, we're running the same SDP as before but without the expectations.

After that, we perform integration tests using notebooks set as tasks within Lakeflow Job. For this job, you’ll need to configure the correct parameters for your target environment (dev, stage, or production). You can run a variety of integration tests, such as counting rows in a table, verifying that tables were created successfully, checking that tables contain the specified columns or distinct values, ensuring column values fall within a certain range, confirming there are no duplicates, and more.

Finally, once the unit tests, data pipeline, and integration tests are successfully executed, the final visualization is created.


# 18. Lecture - Version Control with Git Overview


## A. Complications with Version Control


**Organizational Challenges**

1. **Development Silos**
Silos form over time that isolate development and team operations.

2. **Lower Software Quality**
Independent development leads to lower quality due to duplicate code, inconsistent standards, code reviewing, and more.


3. **Unstable Versioning**

it can become difficult to track, revert and audit changes.

4. **Frequent Updates**


without proper version control, managing frequent updates increases risk.


5. **Scaling Development**


branching, merging and ci/cd integration can become difficult, diminishing the ability to scale development.


## B. Secure Code Changes Through Branching

* complementary concept to ci/cd -> enables effective ci

* version control changes  and run through quality control before merging to main branch and deploying

* example : gitflow

<div align="center">
  <img src="Images/Screenshot%202026-07-23%20at%202.43.20 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


This is an example of a version control and code management over time with Gitflow, which is a common branching strategy that organizes branches for features, releases, and hotfixes.

Here you will see different versions of a feature (v0.1, v0.2, and v1.0).

Starting with the first version v0.1 on the main branch, branching occurs to the feature branch where testing and be properly performed in isolation.

Now that we know how complicated version control can be and we’ve seen an example of what securing code changes looks like, let’s take a look at how Git can be used with Databricks.


## C. Overview of Git with Databricks


git is a free and open source software framework designed to trach changes in source code during software development.


<div align="center">
  <img src="Images/Screenshot%202026-07-23%20at%202.44.28 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


integrate 3rd party tools into your lakeflow jobs that make sense for your organizational needs.


Git Benefits:

Version control enables tracking code changes, facilitating rollback and collaboration. Branching and merging allow multiple developers to work in parallel and integrate changes efficiently. A distributed job ensures each developer has a full local repository, enhancing flexibility and reliability. Git is optimized for high-performance handling of large projects, and its security features use cryptographic integrity checks to prevent data corruption.

Git Tools & Services

GitHub, GitLab, Bitbucket, Azure DevOps

– Cloud-based repositories with CI/CD, issue tracking, and team collaboration.

Git CLI & GUI Clients (e.g., SourceTree, GitKraken, VS Code Git Integration)

– Provides different interfaces for managing repositories.

CI/CD Integration

– Automated testing and deployment pipelines.

Code Review & Collaboration

– Features like pull requests and merge approvals streamline teamwork.

Security & Access Control

– Role-based permissions and audit logs enhance repository security.

Visual Git Client -

Databricks provides a user-friendly interface for common Git operations, which we will discuss about on the next slide.

Seamless Integration -

Users can leverage remote Git repos while developing code inside Databricks notebooks

CI/CD Capabilities -

The repos REST API enables integration of data and AI projects into CI/CD pipelines, allowing users to automate Git Lakeflow Jobs

## D. Generating GitHub Personal Access Token(PAT)
To generate a Personal Access Token (PAT) in GitHub, go to Settings → Developer settings, choose either fine-grained or classic tokens, and click Generate new token. Configure the required repository permissions, copy the token, and use it in Databricks for integration.


## E. Connecting to Databricks with GitHub PAT

Setting up a Personal Access Token (PAT) and connecting to your repositories is simple—click your user icon, go to Settings → Developer settings → Personal access tokens, then copy the token into the provided text box. Once saved, your account is linked and ready to use with your repository


## F. Databricks Git Folders


Databricks Git folders streamline development by tightly integrating version control systems into the Databricks ecosystem, making it easier to manage code collaboratively while adhering to best practices.

Use-cases include:

Collaborative development of machine learning models and ETL pipelines.
Source-controlling SQL queries for analytics workloads.
Automating deployments through CI/CD pipelines.
Walk through the tabs above to see the core Git-folder operations: cloning a repo by pasting its GitHub URL into the Create Git folder dialog, committing and pushing changes from the UI, pulling updates, managing branches, and reviewing a visual diff before you commit


## G. Git-Based Repos in Databricks


<div align="center">
  <img src="Images/Screenshot%202026-07-23%20at%202.47.20 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


1. Aha! Feature:

DB-3749 W2.0: Projects API private preview with tag and commit based checkouts | Aha!

2. Description of the features:

The Repos API provides programmatic access to git-based Re[ps that are part of the Workspace 2.0 effort. With the API, customers can integrate Databricks Repos with their CI/CD workflow. They can programmatically create/update/delete Repos, perform git operations, and specify Git versions when running Jobs based on notebooks in Repos.

3. Value of the feature (aka what databricks was before this feature, and what this feature will do for databricks)

Right now, many customers have built workarounds using the Databricks CLI to pull notebooks from Databricks, check them into Git, pull them from Git, and push them back to Databricks. This is not a very robust solution. With Repos and the Repos API we provide a native feature to pull code from a Git repository, check updates back into Git, and programmatically update this Repos using the Repos API

4. Associated summary slide bullet points: Repos API for CI/CD integration

5. Cloud: All

6. Deployment (MT, ST, Azure): GA

# 19. Lecture - Deploying Databricks Assets Overview


## A. Deployment Options

By the end of this lecture, you will be able to:

Understand and demonstrate the difference between use-cases of the Databricks REST API, CLI, and SDK
Understand how software engineering best practices are supported with DABs
Understand how DABs are used for CI/CD


<div align="center">
  <img src="Images/Screenshot%202026-07-23%20at%202.49.24 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>

**Key comparisons:**

- Ease of use: SDK > CLI > REST API
- Flexibility: REST API > SDK > CLI
**REST API**
Postman and Databricks

**CLI**
Command line

**SDK**
Python, Go, R, Java

**REST API:** Most flexible but complex—best for custom integrations.

**CLI:**Simplifies REST API operations but has limited flexibility.

**SDK:** Most developer-friendly—best for embedding Databricks functionality in applications.


## B. Declarative Automation Bundles and Software Engineering Practices

databricks recommends declartive automation bundles for creating,development, deployment, and testing jobs and other databricks resources.


<div align="center">
  <img src="Images/Screenshot%202026-07-23%20at%202.51.25 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


Declarative Automation Bundles (DABs) are designed to facilitate the adoption of best practices in software engineering, particularly for data and AI projects. Here we will identify 4 core components of SWE practices that are supported by DABs.

- **Version Control:** How are we tracking changes and maintaining a history of code modifications?
- **Code Review:** How are maintaining code quality and are we adhering to coding standards?
- **Testing:** Is our coding behavior consistent and predictable?
- **Continuous Integration:** Are we automating various processes for integrating code changes within our repository?
Declarative Automation Bundles provide a structured approach to managing Databricks projects while adhering to software engineering best practices. By combining infrastructure-as-code principles with automation capabilities, they streamline collaboration, improve quality assurance, and enable efficient delivery of data-driven solutions.

- DABs integrates seamlessly with Git-Based Lakeflow Jobs, enabling users to version their Databricks resources alongside source code
- By treating Databricks resources as code, DABs enables peer review through standard Git Lakeflow Jobs like pull requests
- Developers can use the Databricks CLI with DABs to run tests on bundles in isolated environments, ensuring that Lakeflow Jobs behave as intended
- DABs integrate with CI/CD tools like GitHub Actions or Azure DevOps to automate validation, deployment, and execution of Databricks Lakeflow Jobs


## C. Declarative Automation Bundles


Create code that can be deployed across multiple environments without modification. This ensures consistency, reduces manual errors, and accelerates delivery by automating deployment processes.

DABs are a tool designed to streamline this process for Databricks projects. They enable developers to define Databricks resources (like jobs, pipelines, and notebooks) as source files and metadata in YAML format.

DABs work by first defining your resources and requirements in a databricks.yml file. You then validate the bundle utilizing the Databricks CLI and deploy to your chosen workspace. Once deployed, Lakeflow Jobs or pipelines described in the bundle can be executed

## D. Development and CI/CD with DABs

**Development and CI/CD with DABs**

**Databricks workspaces**


<div align="center">
  <img src="Images/Screenshot%202026-07-23%20at%202.55.02 PM.png" width="800" alt="Databricks Screenshot" style="max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0;" />
</div>


Here we present a high-level view of architecture for development and CI/CD with DABs.

- If you’re working locally, you build the project bundle with your team using a local environment setup.
- Next, you perform version control with project repository, where users commit changes.
- Users can manually deploy to test the changes in their development workspace
- When users commit changes, a notification is triggered to implement the CI/CD pipeline to staging and production.
In summary, Declarative Automation Bundles (DABs) are a tool designed to simplify the management and deployment of data and AI projects on the Databricks platform. They follow an Infrastructure-as-Code (IaC) approach, allowing users to define and manage Databricks resources—such as jobs, pipelines, notebooks, and machine learning models—through YAML configuration files. These bundles streamline collaboration, testing, deployment, and version control across various environments

