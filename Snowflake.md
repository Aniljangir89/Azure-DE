# ❄️ Snowflake — Complete Notes

> Snowflake is a **cloud-based data warehousing platform** that runs on AWS, Azure, and GCP. It is fully managed, scalable, and separates compute from storage.

---

##  What Will You Learn?

| # | Topic | Category |
|---|---|---|
| 1 | Welcome & Getting Started | Basics |
| 2 | Architecture | Core Concepts |
| 3 | Loading Data | Data Ingestion |
| 4 | Copy Options | Data Ingestion |
| 5 | Unstructured Data | Data Types |
| 6 | Performance | Optimization |
| 7 | Load from AWS | Cloud Integration |
| 8 | Load from Azure | Cloud Integration |
| 9 | Load from GCP | Cloud Integration |
| 10 | Snowpipe | Continuous Ingestion |
| 11 | Time Travel | Data Recovery |
| 12 | Fail Safe | Data Recovery |
| 13 | Table Types | Schema Design |
| 14 | Zero-Copy Cloning | Data Management |
| 15 | Data Sharing | Collaboration |
| 16 | Data Sampling | Analytics |
| 17 | Scheduling Tasks | Automation |
| 18 | Visualizations | Reporting |
| 19 | Streams | Change Data Capture |
| 20 | Materialized Views | Performance |
| 21 | Data Masking | Security |
| 22 | Access Management | Security |
| 23 | Partner Connect | Integrations |
| 24 | Best Practices | Guidelines |

---

## 1.  Getting Started

- Snowflake is a **SaaS** (Software as a Service) data platform
- No hardware or software to install or manage
- Supports **multi-cloud**: AWS, Azure, GCP
- Uses **SQL** as query language — easy to adopt
- You can sign up for a **30-day free trial** at [snowflake.com](https://www.snowflake.com)
- you will get free $400 credits for 30 days.
![alt text](images-ss/Screenshot%202026-07-09%20at%204.39.31 PM.png)

-  before executing our query we need to select which role is executing this .
- similary we can choose warehouse and database also.

---

## 2.  Architecture

Snowflake has a **unique 3-layer architecture**:

```
┌─────────────────────────────────┐
│       Cloud Services Layer       │  ← Authentication, Optimizer, Metadata
├─────────────────────────────────┤
│      Query Processing Layer      │  ← Virtual Warehouses (Compute)
├─────────────────────────────────┤
│       Database Storage Layer     │  ← Columnar, Compressed, Micro-partitions
└─────────────────────────────────┘
```

- **Storage Layer** — Data stored as **micro-partitions** in columnar format (compressed)
- **Compute Layer** — **Virtual Warehouses** (independent compute clusters)
- **Cloud Services Layer** — Handles authentication, query optimization, metadata management
- **Key Advantage:** Storage and Compute are **completely separate** → scale independently


- **Schema** : it is container within the database.
- **Table** : it is container within the schema.
- data is not stored in snowflake itself is uses external cloud providers like in our case we selected as default cloud provider aws.
-  and data is stored in **hybrid columnar storage** saved in blobs
- in query procesing layer we can execute the multiple queries parally at a time with multiple warehouses.
- warehouse that are provide us a compute capacity. so we can increase our warehouse size to increase the compute capacity. 
- we can choose the number of servers based on size that we choose like : XS - 1server, S - 2 servers, M - 4 server , L - 8 server, XL - 16 server, 4XL - 128 server like that...

![alt text](images-ss/Screenshot%202026-07-10%20at%201.08.52 PM.png)
- **multiclustering:** - just suppose we are running a query in S sized warehouse but that warehouse is not handling that load  then additional servers can be activae to handle that query load , then they can cluster together to handle that load, by this we can gain more compute power to execute more complex query.it is not runs query faster but it run multiple query at same time.

![alt text](images-ss/Screenshot%202026-07-10%20at%201.26.29 PM.png)

- **Query acceleration**: this is temporary extra compute, if it is needed , so that some slow, outlier query can be accelerated. **Scale factor** - control how much extra compute snowflake can add. 

- we can create warehouse with sql command .

```sql

    create or replace warehouse ANILS_WH
    with    
    warehouse_size = 'X-SMALL',
    min_cluster_count  = 1,
    max_cluster_count  = 3;

```

### Auto Scaling : 

![alt text](images-ss/Screenshot%202026-07-10%20at%201.52.54 PM.png)
- **Standard** : in this policy when load is increased then additional servers can be added and clustered to handle that load, similarly when load is decresed then servers will be removed 
- **Economy:** here it would start an additional cluster only if the system detect that ther is enough query load to keep the cluster busy for atleast 6mint, this also after 5 or 6 consecutive successful checks it will automaticaly shuts down.

### **Database creation:**:
- we can create and manage the database through sql and also through interface also , where we can assing previlege to the other roles like admin, security admin , public like that..

![alt text](images-ss/Screenshot%202026-07-10%20at%202.38.04 PM.png)

- if we want to load the data from aws s3 bucket into our table then we can write this sql query

```sql
 
 COPY INTO LOAN_PAYMENT
    FROM s3://bucketsnowflakes3/Loan_payments_data.csv
    file_format = (type = csv 
                   field_delimiter = ',' 
                   skip_header=1);
    
```

- if i write only loan_payment then is will not a qualified name but if i write like **our_firstdb.public.loan_payment** then it is a qualified name.


- **data warehouse**  : database that is used for reporting and data analysis.


- **Cloud Computing:** company has large amount of data and want to work with that data, but the problem is that this can cause lots of overhead, because data centers are located physically thats why this can  cause lots of overhead , but when we use cloud computing so all this overhead are  managed by the cloud providers and we dont have to worry about all this(like infrastructure, security, electricity, cooling, maintenance etc).
![alt text](images-ss/Screenshot%202026-07-10%20at%204.10.02 PM.png)


---

## Snowflake Editions

Snowflake offers **4 editions** — each builds on top of the previous one.

| Edition | Key Highlights |
|---|---|
| **Standard** | Complete DWH, Auto encryption, Time Travel (1 day), Fail Safe (7 days), Network policies, SSO, 24/7 support |
| **Enterprise** | All Standard + Multi-cluster warehouse, Time Travel (90 days), Materialized views, Search Optimization, Column-level security |
| **Business Critical** | All Enterprise + Customer-managed encryption, Data regulation support, DB failover/failback (disaster recovery) |
| **Virtual Private** | All Business Critical + Dedicated virtual servers, Dedicated metadata store, Completely isolated from all Snowflake accounts |

---

## Snowflake Pricing

Snowflake pricing is based on **3 components**:

| Component | Description |
|---|---|
| **Compute** | Charged per second when virtual warehouse is running |
| **Data Transfer** | Cost for moving data across regions or cloud providers |
| **Storage** | Monthly cost for data stored (compressed size) |

### Key Points
- Compute and Storage costs are **decoupled** — pay for each independently
- **Pay only for what you use** — no upfront cost
- Scalable and affordable across all cloud providers
- Pricing varies depending on the **region and cloud provider** (AWS / Azure / GCP)
- charged price will depend on the region that we have choosed and also cloud provider too.

- and for storage : monthly storage fees, based on avg storage used per month, cloud providers, cost calculated after compression, pay only for what you use

![alt text](images-ss/Screenshot%202026-07-10%20at%204.35.04 PM.png)

- after all of these one thing is remaining still which is **data transfer** : in this normally data ingress is free,but data egress is charged, 

![alt text](images-ss/Screenshot%202026-07-10%20at%204.37.20 PM.png)

![alt text](images-ss/Screenshot%202026-07-10%20at%204.38.57 PM.png)

- this dashboard we can see all the costs how much cost which warehouse is using, and which are the top most expensive queries .
- in this dashboard we can see the cost consumption on all type uses like compute,storage,data transfer.

### **Resource monitors:** 

- controls and monitors credit usage of warehouse and account

- credit quota : number of credits allowed per cycle.

- we can set that monitor type at accound level also at virtual warehouse leve.
- in warehouse i can choose multiple warehouse or single warehouse its our choice.
-  after that we can define some actions 
    * suspend immediately and notify when this % of credit used.
    * suspend and notify this % of credit used.
    * notify when this % of credit used.
- this can be done by only account admin role

### **Roles in snowflake:** 

![alt text](images-ss/Screenshot%202026-07-10%20at%206.07.09 PM.png)

- **Role Hierarchy:** Roles inherit permissions from roles higher in the hierarchy. For example, if Role A is granted Role B, then Role A inherits all privileges of Role B. This allows for granular and flexible permission management.


- we can grant roles to users and revoke also for some privileges

- every query is executed with some role
- when some user create the object then the role of that user is also added as a auther of that object.

- on is **orgadmin** which manages actions on organisation level
    * he can create accounts
    * view all the accounts. 

- **accoundadmin** : top level role
    * should be to limited number of users
    * contains securityadmin and system admin
    * can manged all objects in account
    * include share and reader account.

- **securityadmin** : manages any object grant globally
    * manage grants privilege
    * create, monitor, and mange user and roles.
    * inherits useradmin privileges

- **sysadmin:**: create warehouse, database and other objects

    * all custom roles should be assigned to 
    * can grant privileges on warehouses,databases and other objects.

- **useradmin:** dedicated to usr and roles management

    * create user and create roles privileges
    * can manage users and roles that are owned
- **public:** automatically granted per default
    * grant to when no access control needed.
    * object can be owned but are available to everyone.
---



## 3. Loading Data

### Bulk Loading (COPY INTO)

- in bulk loading we basically 
    * most frequent method
    * uses warehouses
    * loading fro stages
    * copy command is used
    * transformations can be included  

- in continus loading
    * designed to load small volumes of data
    * automatically once they added to stages
    * lates result of analysis
    * snowpipe(serverless feature)

- Use `COPY INTO` command to load data from **staged files**
- Supports formats: **CSV, JSON, Parquet, Avro, ORC, XML**


### understanding stages
- not to be confused with datawarehouse
- stage is general concept 
- stage is basically location of the object 
- location of data files where data can be loaded from

* there are 2 type of stages :

1. External stages : point to cloud storages like S3, Azure Blob, GCS
    * when we create external stages we have to provide location of the files.
    * we have to provide credentials to access the files.
2. Internal stages : internal stages are managed by snowflake itself.
    * user level internal stage : when we upload files
    * table level internal stage :
    * named internal stage : is created by snowflake admin and can be used by all of the users in the organization.

- in copy command of the snowflake is very intersting because it collects the metadata,to prevent duplicate copying.
```sql
COPY INTO my_table
FROM @my_stage/myfile.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);
```

### Stages
- **Internal Stage** — Snowflake managed storage (`@~`, `@%table`, `@stage_name`)
- **External Stage** — Points to S3, Azure Blob, GCS bucket

### how to handle errors during copying the data from external stages:

- if files contains some column that does not meet the creteria of our example table then definatly we will get error while copyin the data from external stage

```md
    Numeric value 'one thousand' is not recognized File 'OrderDetails_error.csv',
```
- because in order table the price value supposed to be int but external stage contains also textual values such as 'one thousand'




### to handle this kind of problem :

1. use on_error = continue  : option in which we load only that rows which are passed the validation and skip other rows
![alt text](images-ss/Screenshot%202026-07-13%20at%202.20.52 PM.png)

2. use on_error = obort_statement : in this does't matter how many files are there in external stage if one of them contains wrong values then it will fail to load any files

3. use on_error = skip_file : this means it load only that files which pass the valid checks

![alt text](images-ss/Screenshot%202026-07-13%20at%203.20.02 PM.png)



---

## 4. ⚙️ Copy Options

Key options used with `COPY INTO`:

| Option | Description |
|---|---|
| `ON_ERROR` | What to do on error: `CONTINUE`, `SKIP_FILE`, `ABORT_STATEMENT` |
| `PURGE` | Delete files from stage after loading (`TRUE`/`FALSE`) |
| `FORCE` | Re-load already loaded files |
| `VALIDATION_MODE` | Validate without loading: `RETURN_ERRORS`, `RETURN_ALL_ERRORS` |
| `SIZE_LIMIT` | Limit data loaded in bytes |


### **working with rejected records
- just take and example that while copying the data some of the records get failed while loading then ...??
- we can create a sapared table for that named table_rejected , then we can select only that column which contains rejected_records and we can find that records from prev query
- then we loaad that records into rejected table 

```sql
create or replace table rejected 
as 
select rejected_record from table(result_scan(last_query_id()));
```
![alt text](images-ss/Screenshot%202026-07-14%20at%201.31.11 PM.png)


- if we use **on_error =continue** in our copy command then it will skip some records which are rejected but we can see these result using validatio mode
```sql
    COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    ON_ERROR=CONTINUE;
  
  
select * from table(validate(orders, job_id => '_last'));
```

-- but there is some problem like all the rejected records are in one column for that we have to make splite them 

```sql

    CREATE OR REPLACE TABLE rejected_values as
SELECT 
SPLIT_PART(rejected_record,',',1) as ORDER_ID, 
SPLIT_PART(rejected_record,',',2) as AMOUNT, 
SPLIT_PART(rejected_record,',',3) as PROFIT, 
SPLIT_PART(rejected_record,',',4) as QUATNTITY, 
SPLIT_PART(rejected_record,',',5) as CATEGORY, 
SPLIT_PART(rejected_record,',',6) as SUBCATEGORY
FROM rejected; 
```


### **copy options** :

1. **size_limit:**
```sql 
    copy into <table_name>
    from externalStage
    files = ('<file_name>','<file_name>')
    size_limit = num

```
- specify max size(in B) of data loaded in the command(at least one file)
- just take an example that  there a 3 files in the stage and i want to copy that files, and size limit is 30000, then i load the file from start to end files till its size exceeded the size_limit.

2. **return failed only:**

```sql
    COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    ON_ERROR =CONTINUE
    RETURN_FAILED_ONLY = TRUE;
```
- its better to use return failed with on error becuase it directly give you the rejected records in the result

![alt text](images-ss/Screenshot%202026-07-14%20at%202.11.12 PM.png)

3. **TruncateColumn**

- specifies whether to truncate text strings that exceed the target column lenght

- TRUE : stringg are automatically truncated to the target column lenght

- False: copy produces an error if a loaded string exceeds the targed column length.


4. **Force:**
- specifies to load all files, regardless of whether they have been loaded previously and not changed since they were loaded

- force = true : option reloads files, potentially duplicating the data in table
- force = false  : option will not reload the files which are already loaded previously 



### **Load History:**

- enable you to retrieve the history of data loaded into table using copy into command.

```sql
    select * from COPY_DB.INFORMATION_SCHEMA.LOAD_HISTORY;
```
- we can also load the history globally from snowflake database

```sql

    select * from SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY;
```
---



## 5.  Unstructured Data

- Snowflake supports **semi-structured data**: JSON, Avro, ORC, Parquet, XML
- Stored in **VARIANT** data type column
- Use **dot notation** or **`:`** to query nested fields

```sql
SELECT raw:name::STRING, raw:age::INT
FROM my_json_table;
```

---

## 6. ⚡ Performance

- **Virtual Warehouses** — Scale up (larger size) or scale out (multi-cluster)
- **Warehouse Sizes**: X-Small → X-Large → 4X-Large
- **Auto-suspend / Auto-resume** — Saves cost when idle
- **Result Cache** — Identical queries reuse cached results (24 hrs)
- **Clustering Keys** — Optimize large table scan performance
- **Search Optimization Service** — Speeds up selective point lookup queries

---

## 7. ☁️ Load from AWS (S3)

```sql
-- Create external stage pointing to S3
CREATE STAGE my_s3_stage
URL = 's3://my-bucket/data/'
CREDENTIALS = (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...');

-- Load data
COPY INTO my_table FROM @my_s3_stage;
```

- Use **IAM Roles** for secure access (recommended over keys)
- Supports **S3 Event Notifications** with Snowpipe for auto-ingestion

---

## 8. 🔵 Load from Azure (Blob Storage)

```sql
-- Create external stage pointing to Azure Blob
CREATE STAGE my_azure_stage
URL = 'azure://myaccount.blob.core.windows.net/mycontainer/data/'
CREDENTIALS = (AZURE_SAS_TOKEN = '...');

-- Load data
COPY INTO my_table FROM @my_azure_stage;
```

- Uses **SAS Tokens** or **Azure Service Principal** for authentication
- Integrate with **Azure Event Grid** for Snowpipe auto-ingest

---

## 9. 🟢 Load from GCP (Google Cloud Storage)

```sql
-- Create external stage pointing to GCS
CREATE STAGE my_gcs_stage
URL = 'gcs://my-bucket/data/'
STORAGE_INTEGRATION = my_gcs_integration;

-- Load data
COPY INTO my_table FROM @my_gcs_stage;
```

- Uses **Storage Integration** objects (recommended — avoids storing credentials)
- Supports **GCS Pub/Sub notifications** for Snowpipe

---

## 10. 🔄 Snowpipe (Continuous Data Ingestion)

- **Snowpipe** automatically loads data as soon as new files arrive in the stage
- Serverless — no warehouse needed (Snowflake manages compute)
- Works with **cloud event notifications** (S3, Azure Event Grid, GCS Pub/Sub)

```sql
-- Create a pipe
CREATE PIPE my_pipe AS
COPY INTO my_table
FROM @my_stage
FILE_FORMAT = (TYPE = 'CSV');
```

- Monitor pipes: `SYSTEM$PIPE_STATUS('my_pipe')`
- **Use case**: Real-time/near-real-time data ingestion pipelines

---

## 11. ⏪ Time Travel

- Allows you to **access historical data** at any point within a retention period
- Default retention: **1 day** (Enterprise: up to **90 days**)

```sql
-- Query data as of a specific time
SELECT * FROM my_table AT (TIMESTAMP => '2024-01-01 10:00:00'::TIMESTAMP);

-- Query data before a change
SELECT * FROM my_table BEFORE (STATEMENT => '<query_id>');

-- Restore a dropped table
UNDROP TABLE my_table;
```

- **Use cases**: Undo mistakes, audit data changes, recreate dropped objects

---

## 12. 🛡️ Fail Safe

- **Fail Safe** is a **7-day** (non-configurable) recovery period **after Time Travel expires**
- Only accessible by **Snowflake Support** — not available to end users
- Protects against catastrophic failures / data corruption
- **Not intended for regular recovery** — use Time Travel for that

```
Time Travel (0–90 days)  →  Fail Safe (7 days)  →  Data permanently deleted
```

---

## 13. 🗂️ Table Types

| Table Type | Persistence | Time Travel | Fail Safe | Use Case |
|---|---|---|---|---|
| **Permanent** | Until dropped | ✅ Yes | ✅ Yes | Production data |
| **Transient** | Until dropped | ✅ Limited (1 day) | ❌ No | Intermediate/staging data |
| **Temporary** | Session only | ✅ Limited (1 day) | ❌ No | Session-scoped temp work |

```sql
CREATE TRANSIENT TABLE my_staging_table (...);
CREATE TEMPORARY TABLE my_temp_table (...);
```

---

## 14. 🔁 Zero-Copy Cloning

- Creates a **copy of a database, schema, or table instantly** — without copying actual data
- Uses **metadata pointers** — only new changes consume additional storage
- **No extra cost** at clone creation time

```sql
-- Clone a table
CREATE TABLE my_table_clone CLONE my_table;

-- Clone a schema
CREATE SCHEMA my_schema_clone CLONE my_schema;

-- Clone a database
CREATE DATABASE my_db_clone CLONE my_db;
```

- **Use cases**: Dev/test environments, backup before bulk updates, sandboxing

---

## 15. 🤝 Data Sharing

- Share **live, real-time data** with other Snowflake accounts — **no data movement or copying**
- Uses **Secure Views** and **Shares** objects
- Consumer sees read-only access to provider's data

```sql
-- Provider creates a share
CREATE SHARE my_share;
GRANT USAGE ON DATABASE my_db TO SHARE my_share;
GRANT SELECT ON TABLE my_table TO SHARE my_share;
ALTER SHARE my_share ADD ACCOUNTS = <consumer_account>;
```

- **Snowflake Marketplace** — publish data products for others to discover & use

---

## 16. 🔬 Data Sampling

- Allows querying a **random subset** of rows — useful for large table analysis
- Two methods:

```sql
-- BERNOULLI / ROW sampling (row by row probability)
SELECT * FROM my_table SAMPLE BERNOULLI (10);  -- ~10% of rows

-- SYSTEM / BLOCK sampling (faster, block-level)
SELECT * FROM my_table SAMPLE SYSTEM (10);     -- ~10% of micro-partitions
```

---

## 17. ⏰ Scheduling Tasks

- **Tasks** allow scheduling SQL statements or stored procedures automatically
- Similar to a cron job inside Snowflake

```sql
-- Create a task
CREATE TASK my_task
  WAREHOUSE = my_wh
  SCHEDULE = 'USING CRON 0 * * * * UTC'   -- every hour
AS
  INSERT INTO my_summary SELECT * FROM my_source;

-- Resume the task (tasks start suspended by default)
ALTER TASK my_task RESUME;
```

- **Task Trees** — chain tasks together (parent-child dependency)
- Serverless tasks also available (no warehouse needed)

---

## 18. 📊 Visualizations

- Snowflake has a built-in **Snowsight** UI for charting query results
- Supports: Bar, Line, Scatter, Heatmap charts directly in the worksheet
- For advanced BI, integrate with:
  - **Power BI**
  - **Tableau**
  - **Looker**
  - **Streamlit** (now natively embedded in Snowflake)

---

## 19. 🌊 Streams (Change Data Capture)

- **Streams** track **INSERT, UPDATE, DELETE** changes on a table
- Enables **CDC (Change Data Capture)** pipelines

```sql
-- Create a stream on a table
CREATE STREAM my_stream ON TABLE my_table;

-- Query changes captured by the stream
SELECT * FROM my_stream;
-- Columns added: METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID
```

- Stream is consumed when used in a DML transaction (INSERT/MERGE)
- **Use case**: Incrementally process only new/changed data in ETL pipelines

---

## 20. 👁️ Materialized Views

- Pre-computed result set that is **automatically refreshed** when base table changes
- Faster query performance for complex aggregations/joins

```sql
CREATE MATERIALIZED VIEW my_mv AS
SELECT region, SUM(sales) AS total_sales
FROM orders
GROUP BY region;
```

- Snowflake **automatically maintains** the MV in the background
- Incurs storage and compute cost for maintenance
- **Use case**: Dashboards and frequently run expensive queries

---

## 21. 🔒 Data Masking

- **Dynamic Data Masking** — masks sensitive data at query time based on user role
- Real data stored; masking applied **on the fly** without storing masked copy

```sql
-- Create masking policy
CREATE MASKING POLICY email_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('ANALYST') THEN '***@***.com'
    ELSE val
  END;

-- Apply policy to a column
ALTER TABLE customers MODIFY COLUMN email SET MASKING POLICY email_mask;
```

- **Use case**: GDPR/HIPAA compliance, protecting PII

---

## 22. 🔐 Access Management

- Snowflake uses **RBAC (Role-Based Access Control)**
- Key built-in roles:

| Role | Description |
|---|---|
| `ACCOUNTADMIN` | Highest privilege — account level admin |
| `SYSADMIN` | Creates and manages warehouses and databases |
| `SECURITYADMIN` | Manages users and roles |
| `USERADMIN` | Creates users and roles |
| `PUBLIC` | Default role for all users |

```sql
-- Grant role to user
GRANT ROLE analyst TO USER john;

-- Grant privilege on table
GRANT SELECT ON TABLE sales TO ROLE analyst;
```

- **Row-Level Security** via Secure Views or Row Access Policies

---

## 23. 🔗 Partner Connect

- **Partner Connect** — one-click integration with 3rd-party tools directly from Snowflake UI
- Available partners include:

| Category | Tools |
|---|---|
| ETL/ELT | Fivetran, dbt, Matillion |
| BI | Tableau, Looker, Power BI |
| Data Science | DataRobot, Hex |
| Security | Alation, Collibra |

- Snowflake automatically creates trial accounts and configures connections

---

## 24. ✅ Best Practices

### Cost Optimization
- Use **auto-suspend** and **auto-resume** on warehouses
- Use **Transient tables** for staging data (no Fail Safe cost)
- Monitor credit usage with **Resource Monitors**

### Performance
- Use **clustering keys** for very large tables that are frequently filtered
- Avoid `SELECT *` — query only needed columns
- Use **result caching** — avoid redundant queries
- Right-size your **Virtual Warehouse** — bigger is not always better

### Security
- Follow **least privilege** principle for role assignments
- Enable **MFA** for all users
- Use **Network Policies** to restrict access by IP
- Apply **Dynamic Data Masking** for PII columns

### Data Loading
- Load files between **100 MB – 250 MB** (compressed) for optimal performance
- Use **Snowpipe** for continuous/streaming ingestion
- Always validate with `VALIDATION_MODE` before full load

---

> 💡 **Summary:** Snowflake's power lies in its **separation of storage and compute**, native **multi-cloud support**, and enterprise-grade features like **Time Travel, Zero-Copy Cloning, Data Sharing, and Dynamic Data Masking** — making it one of the most popular cloud data platforms today.
