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
    * format_option() : lets you control the control behavior of the copyinto operation itself,for example schema evaluation using mergeSchema.
3. **Auto Loader** : 
    * auto loader increamently and effiecently load new data files in either in batch or streaming mode as they arrives in cloud object storage,and it does this without any additional steps.
    ![alt text](Images/auto_loader.png)
    ![alt text](Images/autoloader1.png)
    * cloudfiles.format : it tells auto loader to what type of files to expect in the directory, such as csv, json, parquet, etc. This setting is necessary for the auto loader to properly parse and process the files.
    * schemalocation:save or remember the structure(schema) of the data here, so i dont re detect it every time.
    * trigger( time = x seconds): check for new files every 5 second and process them - the hearbeat interval is the time gap between each check. 
    * to create streaming table from the files in volume you use auto loader , databricks recommands using auto loader with lakeflow declrative pipelines for most of data ingestion tasks from the cloaud storage
    * streaming table : this is delta table that continously updates itself as new data is ingested into it, and allows you to query the data in real-time.
    * streaming tables in databricks sql are backed by serverless lakeflow declarative pipelines. your workspace must support serverless piplines to use this functionality. alternatively, use can build your own lakeflow declarative pipelines for incremental processing, optimzation and monitoring. 
    ![alt text](Images/ingestion_summary.png)

---

## Appending metadata column on ingestion 

![alt text](Images/meta_data_image.png)
* you can append metadata column info  from input data source files when creating table .
* this is very important for tracking the info of table ,auditing,lineage, and debugging purpose.

## working with rescued data column 

* during ingestion there are times when input data does not match with schema in your table. ingestion technique like read_file(),spark.read, autoloader provide rescued sdata column during ingestion.this make sure that incoming data which fail in schema validation is not lost.
![alt text](Images/rescued_data.png)
![alt text](Images/rescued_data1.png)
* when we ingest data then user column must read as string in table and cost column must be in Bigint 
* see in first row user column passed the validation but cost column contain value as string like $100 which is failed to load because expection is like that in bigint
* so instead of simply droping that column simple we add new column name rescued data where we add this cost as json format and actual cost column filled with null value.

## Ingestion json formated data

* ![alt text](Images/Screenshot%202026-06-10%20at%206.43.25 PM.png)
* we deal with this type of data commanly when dealing with event data, logs, data from apis
* these object can be flat means all the key: value pair can be in single level,or they can be in nested structure, where value also can be in nested structure.
* ![alt text](Images/Screenshot%202026-06-10%20at%206.46.39 PM.png)
* its common that after ingestion one or more column in your table might cantain json-format string as values.
* so the techniques to parse these json objects,extract,manipulate these json string using sql or dataframe operation.so that you can access these key value pair like a common column field.
* ![alt text](Images/Screenshot%202026-06-10%20at%206.50.26 PM.png)
* a key point to remember:
    1. a column can store json string or json object as string.
    2. so its just row text from system perspective.
* to access subfield from json formated string column, you can use the colon(:) syntax.
* ![alt text](Images/Screenshot%202026-06-10%20at%206.54.26 PM.png)
* lets go through the process of mapping json formatted string into struct column 
    * so the first step to define json formatted schema of json formatting string .
    * defining the schema allow you to tell databricks how to interpret each part of json string and how to convert these into appropriate data type within a struct.
    * ![alt text](Images/Screenshot%202026-06-10%20at%206.58.28 PM.png)
    * ![alt text](Images/Screenshot%202026-06-10%20at%207.00.00 PM.png)
* these can be done with these 2 steps:
    - first step is to get schema of the json format string
    - now instead of manually defining schema, you can use builtin schema_of_json() function to automatically define the schema of the example json string.
    ```sql
   select schema_of_json(json_col, 'json-struct-schema') as struct_column
   from table;
    ```

![alt text](Images/Screenshot%202026-06-10%20at%207.13.10 PM.png)
* some major benifits of variant  data type includes:
    - it can store any type of data including json, making ideal for semi-sturctured data.
    - it is highly flexible, it can adapting to different data shapes without rigit schemas
    - if offers improved performace as compared to existing methods for handling semi-structred data.



    