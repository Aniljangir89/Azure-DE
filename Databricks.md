# Databricks
---
## Databricks connectors:
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

![alt text](Images/image%20copy.png)

![alt text](Images/connectors.png)

### ingestion methods
* when ingesting data into databricks using  lakeflow connect standard connectors, you can choose from serveral methods.
1. batch ingestion : loads the data in batches of the rows based on the schedules, but while loading the data it load whole data every time.
![alt text](Images/batch_ingestion%20.png)

2.increamental batch ingestion: automaticaly detect new records in the data sources and skips the record which are already loaded in previous batches. this is fast and resource efficient.
![alt text](Images/increamental_batchs.png)
3. streaming ingestion : in this data is continuasly loaded as it generated, and allow use to query in real time, this method is ideal for loading data from source like apache kafka, amazon kinesis, pub/sub,and apache pulsar.



---
## Delta lake :
* Delta Lake is an open-source storage layer that sits on top of your data lake (like Azure Data Lake / S3) and adds reliability, transactions, and versioning to your data files.

* the goal is to ingest the data from external source like cloud storage into delta lake as delta tables, remember delta lake is simplly open source protocal to read and write data into cloud storage.

![alt text](Images/deltalake.png)

* under the hood , delta tables store the data in within a folder directory, within the folder directory data is stored are parquet files,and what delta adds is delta logs stored as json files along with its parquet files. these logs keeps the track of all the transactions of the data of parquet files and table versions.
* within the transation log, we now have the concept of the table states,so now if you insert, delete, update the data of your tables, delta basically add the transation(the log file) and your table stays updated and managed.
* so with the transation log you are able to easly gets views of your data,and you can able to **travel back** in time. 
![alt text](Images/deltalake1.png)

![alt text](Images/delta_features.png)

1. ACID : with the help of this multiple users can perform operation concurrently, and there will be no data loss
2. DML : we can perform operations such as insert, delete, update and merge, enabling flexible data management.
3. Time travel : we can query and rever prev versions of the data, facilitating auditing and recovery.
4. Schema Enforcement and Evolution : define schema for data integrity, delta tables will validate the data before writing into the tables,while evolutin mean we can add new columns in the table without breaking workflow.

![alt text](Images/medalian_architecture.png)
* As you ingest the data in to your delta lake through batch or streaming ingestion,or both . then you can begin processing and transforming your data into databricks.
* it begins with bronze layer, the row data ingestion layer,this layer ingest the row and unprocessed data from different data sources,and serving the fundamental storage for all data.
* in silver layer the data is cleaned, transformed and enriched and provied more refined dataset for later analysis.
* later gold layer contains business specific data, aggregated, and ready for business decision. 

---
## Data ingestion from cloud storage :

* data ingested from claud storage, row files are effieciently  converted into delta tables using databricks tools.

![alt text](Images/data_ingestion.png)

* we discussed about 3 methods of the data ingestion from cloud storage into delta tables.
    1. create table as(ctas)
    2. copy into 
    3. auto loader

 
1. **create table as(ctas)**:
   * this stmt creates a delta table by default from files stored in claud storage.

   ![alt text](Images/ctas.png)
   * the read_files() function  is used read files from specific locaion(claud storage) and return the data in table format, it offers several capabilities.
     1. support varias file format
     2. automatically detect file format and and infer unified schema across all files.
     3. can be used in streaming tables to increamently ingest files into delta lake using auto loader
2. **copy into** :
    * this command performs bulk load from files in cloud storage into the tables.
    ![alt text](Images/copyinto%20.png)
    * means copy into skips any files that already been loaded into the table, and only new files will be ingested .
