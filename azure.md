# Azure Core Fundamentals: Account Setup, Resource Groups, Storage & IAM (RBAC)

---

## Table of Contents
1. [Azure Account Hierarchy & Setup](#1-azure-account-hierarchy--setup)
2. [Azure Resource Groups (RG)](#2-azure-resource-groups-rg)
3. [Azure Storage Accounts (Blob & GPv2)](#3-azure-storage-accounts-blob--gpv2)
4. [Azure Data Lake Storage Gen2 (ADLS Gen2)](#4-azure-data-lake-storage-gen2-adls-gen2)
5. [Identity & Access Management (IAM) & RBAC](#5-identity--access-management-iam--rbac)
6. [Data Engineering Best Practices & Architecture Patterns](#6-data-engineering-best-practices--architecture-patterns)
7. [Azure Data Lake Storage Gen2 Concepts (In Simple Terms)](#7-azure-data-lake-storage-gen2-concepts-in-simple-terms)
8. [Azure Data Factory (ADF) Basics](#8-azure-data-factory-adf-basics)
9. [Azure Key Vault (AKV) & Secret Management](#9-azure-key-vault-akv--secret-management)
10. [Azure Networking Basics for Data Engineering](#10-azure-networking-basics-for-data-engineering)
11. [End-to-End Secure Data Engineering Architecture](#11-end-to-end-secure-data-engineering-architecture)

---

## 1. Azure Account Hierarchy & Setup

Understanding the cloud administrative structure is essential for governing resources, managing costs, and enforcing security policies across an enterprise.

```
                      +-----------------------------+
                      |   Microsoft Entra ID Tenant |
                      |    (Identity & Auth Scope)  |
                      +--------------+--------------+
                                     |
                      +--------------v--------------+
                      |     Root Management Group   |
                      +--------------+--------------+
                                     |
                      +--------------v--------------+
                      |      Management Groups      |
                      |  (e.g., Workloads, Prod)    |
                      +--------------+--------------+
                                     |
                      +--------------v--------------+
                      |        Subscriptions        |
                      |    (Billing Boundary)       |
                      +--------------+--------------+
                                     |
                      +--------------v--------------+
                      |       Resource Groups       |
                      |   (Logical Container)       |
                      +--------------+--------------+
                                     |
        +----------------------------+----------------------------+
        |                            |                            |
+-------v-------+            +-------v-------+            +-------v-------+
|  ADLS Gen2    |            | Data Factory  |            |   Databricks  |
| (Storage Acc) |            |    (ADF)      |            |  Workspace    |
+---------------+            +---------------+            +---------------+
```

### 1.1 Key Hierarchy Components

| Level | Description | Key Governance Use Case |
| :--- | :--- | :--- |
| **Tenant** | Top-level instance of Microsoft Entra ID (formerly Azure AD). Represents the organization. | Single identity boundary for users, groups, and applications. |
| **Management Groups** | Containers that help manage access, policy, and compliance across multiple subscriptions. Can be nested up to 6 levels deep. | Apply Azure Policies or RBAC role assignments across multiple subscriptions simultaneously. |
| **Subscription** | Logical container that links Azure accounts to billing and resource limits. | Primary **billing boundary** and hard limit quota boundary. Separates environments (Dev, QA, Prod). |
| **Resource Group** | Logical deployment container grouping related Azure resources for lifecycle management. | Managing deployment lifecycle, resource locks, and joint RBAC policies. |
| **Resource** | Individual service instance (e.g., Storage Account, Synapse Workspace, VM). | Actual compute, storage, or analytics asset. |

### 1.2 Account Types & Offers
* **Free Account**: Includes 12 months of popular free services, $200 credit for 30 days, and 55+ always-free services.
* **Pay-As-You-Go (PAYG)**: Micro-billed based on hourly/secondly consumption with no upfront cost.
* **Enterprise Agreement (EA)** / **Microsoft Customer Agreement (MCA)**: Negotiated enterprise pricing, committed monetary spending, centralized enterprise portal billing.

### 1.3 Setting Up Azure for Data Engineering (Initial Steps)
1. **Create Entra ID Tenant**: Set up primary domain (e.g., `company.onmicrosoft.com` or custom domain).
2. **Setup Subscriptions**: Create segregated subscriptions (e.g., `sub-data-dev-001`, `sub-data-prod-001`).
3. **Register Core Resource Providers**: Enable service APIs required for data workloads via CLI:
   ```bash
   az provider register --namespace Microsoft.Storage
   az provider register --namespace Microsoft.DataFactory
   az provider register --namespace Microsoft.Databricks
   az provider register --namespace Microsoft.Synapse
   az provider register --namespace Microsoft.EventHub
   ```
4. **Set Up Spending Limits & Budgets**: Configure Azure Cost Management budgets and alerts at the subscription level.

---

## 2. Azure Resource Groups (RG)

A **Resource Group** is a logical container into which Azure resources are deployed, managed, and monitored as a unified entity.

```
       +-------------------------------------------------------------+
       | Resource Group: rg-analytics-prod-eastus-001               |
       | Location: East US                                           |
       | Tags: Environment=Prod, CostCenter=DE-101                   |
       |                                                             |
       |   +-------------------+    +----------------------------+   |
       |   | ADLS Gen2 Storage |    | Azure Data Factory (ADF)   |   |
       |   | stadlsanalytics01 |    | adf-analytics-prod-001     |   |
       |   +-------------------+    +----------------------------+   |
       |                                                             |
       |   +-------------------+    +----------------------------+   |
       |   | Databricks WS     |    | Key Vault                  |   |
       |   | dbw-analytics-prod|    | kv-analytics-prod-001      |   |
       |   +-------------------+    +----------------------------+   |
       +-------------------------------------------------------------+
```

### 2.1 Core Rules of Resource Groups
* **Unique Membership**: Every resource **must** belong to exactly one Resource Group.
* **Cross-Region Support**: Resources inside a Resource Group can reside in **different regions** than the Resource Group itself (the RG location stores deployment metadata).
* **Unified Lifecycle**: Deleting a Resource Group triggers an automatic cascade deletion of **all** contained resources.
* **Resource Mobility**: Most resources can be moved between Resource Groups or Subscriptions (subject to service restrictions).

### 2.2 Resource Locks
Resource Locks prevent accidental deletion or modification of critical production resources.

```
                     +---------------------------------------+
                     |            Resource Lock              |
                     +-------------------+-------------------+
                                         |
            +----------------------------+----------------------------+
            |                                                         |
  +---------v---------+                                     +---------v---------+
  |   CanNotDelete    |                                     |     ReadOnly      |
  |  (Delete Lock)    |                                     |  (ReadOnly Lock)  |
  +---------+---------+                                     +---------+---------+
  | Authorized users  |                                     | Authorized users  |
  | can READ & MODIFY |                                     | can ONLY READ.    |
  | resources, but    |                                     | CANNOT modify or  |
  | CANNOT delete.    |                                     | delete resources. |
  +-------------------+                                     +-------------------+
```

* **Inheritance**: Locks applied at higher scopes (Subscription / Resource Group) are automatically inherited by all child resources.
* **Overriding**: Even users with `Owner` role cannot delete a locked resource until the lock is explicitly removed.

#### Creating a Resource Lock via Azure CLI:
```bash
# Apply CanNotDelete lock on a Resource Group
az lock create \
  --name "LockProdRG" \
  --resource-group "rg-analytics-prod-eastus-001" \
  --lock-type CanNotDelete
```

### 2.3 Resource Tagging Strategy
Tags are key-value pairs assigned to resources and resource groups for metadata organization, cost allocation, and automated operations.

#### Standard Tag Schema for Data Engineering:
```json
{
  "Environment": "Production",
  "Project": "Customer360",
  "Owner": "data-engineering-team@company.com",
  "CostCenter": "CC-8942",
  "DataClassification": "Confidential",
  "AutomatedShutdown": "False"
}
```

---

## 3. Azure Storage Accounts (Blob & GPv2)

Azure Storage is Microsoft's cloud storage solution offering scalable, highly available, and secure object storage.

### 3.1 Storage Account Types

| Storage Account Type | Supported Services | Recommended Usage |
| :--- | :--- | :--- |
| **General Purpose v2 (GPv2)** | Blob, ADLS Gen2, File, Queue, Table | **Default standard account** for most workloads (Data Lakes, Blob storage). |
| **Premium Block Blobs** | Blob, ADLS Gen2 | High transaction rates, low-latency analytics, small file transformations. |
| **Premium Page Blobs** | Page Blobs only | Unmanaged OS/Data disks for Virtual Machines. |
| **Premium File Shares** | Azure Files | High-performance enterprise file shares (NFS / SMB). |

### 3.2 Performance Tiers
* **Standard**: Powered by traditional Hard Disk Drives (HDD) or magnetic/standard SSDs. Optimized for high capacity and bulk storage at low cost.
* **Premium**: Powered by solid-state drives (SSDs). Delivers low latency, high throughput, and high IOPS for demanding analytical workloads.

---

### 3.3 Data Redundancy & Replication Options

Azure automatically maintains multiple copies of your data to guard against hardware failures, network outages, and natural disasters.

```
Primary Region Data Center                       Secondary Paired Region
+------------------------------------+          +------------------------------------+
| Zone 1        Zone 2        Zone 3 |          | Zone 1        Zone 2        Zone 3 |
| +--------+   +--------+   +--------+ |        | +--------+   +--------+   +--------+ |
| | Copy 1 |   | Copy 2 |   | Copy 3 | |        | | Copy 1 |   | Copy 2 |   | Copy 3 | |
| +--------+   +--------+   +--------+ |        | +--------+   +--------+   +--------+ |
+------------------+-----------------+          +------------------------------------+
                   |                                             ^
                   +=========== Geo-Replication =================+
                                  (Async)
```

| Redundancy Option | Description | Copies | Availability Zones | Durability (Annual) | Secondary Region Read |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **LRS** (Locally-Redundant) | Synchronous replication within a single physical data center. | 3 | 1 Zone | 99.999999999% (11 9's) | No |
| **ZRS** (Zone-Redundant) | Synchronous replication across 3 separate Availability Zones in primary region. | 3 | 3 Zones | 99.9999999999% (12 9's) | No |
| **GRS** (Geo-Redundant) | LRS in primary region + Asynchronous LRS to a secondary paired region. | 6 | 1 Primary + 1 Secondary | 99.99999999999999% (16 9's) | No |
| **RA-GRS** (Read-Access GRS) | GRS replication with read-only access to secondary region endpoint. | 6 | 1 Primary + 1 Secondary | 16 9's | **Yes** |
| **GZRS** (Geo-Zone-Redundant) | ZRS in primary region + Asynchronous LRS to a secondary paired region. | 6 | 3 Primary + 1 Secondary | 16 9's | No |
| **RA-GZRS** | GZRS replication with read-only access to secondary region endpoint. | 6 | 3 Primary + 1 Secondary | 16 9's | **Yes** |

---

### 3.4 Access Tiers for Blob Data

Blob storage provides different access tiers to optimize storage costs based on data usage patterns.

```
Access Frequency:   HIGH  ----------------------------------------------->  VERY LOW
Latency:            MS    ----------------------------------------------->  HOURS
Storage Cost:       HIGH  ----------------------------------------------->  LOWEST
Access Cost:        LOW   ----------------------------------------------->  HIGHEST

                +---------+      +---------+      +---------+      +---------+
                |   Hot   | ---> |  Cool   | ---> |  Cold   | ---> | Archive |
                +---------+      +---------+      +---------+      +---------+
                (Active)         (min 30d)        (min 90d)        (min 180d)
```

| Tier | Usage Scenario | Min Stored Duration | Latency | Early Deletion Fee |
| :--- | :--- | :--- | :--- | :--- |
| **Hot** | Frequently read/written data (e.g., active lakehouse data, staging). | N/A | Milliseconds | None |
| **Cool** | Data accessed infrequently; stored for at least 30 days (e.g., monthly reports). | 30 days | Milliseconds | Yes (if deleted before 30d) |
| **Cold** | Rarely accessed data stored for at least 90 days (e.g., backup archives). | 90 days | Milliseconds | Yes (if deleted before 90d) |
| **Archive** | Offline compliance data, historical raw backups. Stored for at least 180 days. | 180 days | Hours (Rehydration required) | Yes (if deleted before 180d) |

> [!IMPORTANT]
> **Archive Tier Rule**: Blobs in the Archive tier are **offline** and cannot be read or modified directly. You must first **rehydrate** the blob to Hot or Cool tier (Standard rehydration: 15 hours; High priority: < 1 hour).

---

### 3.5 Storage Security & Authentication Options

```
                            Authentication Mechanisms
                                        |
       +--------------------------------+--------------------------------+
       |                                |                                |
+------v-------+                 +------v-------+                 +------v-------+
|  Account Key |                 |      SAS     |                 |   Entra ID   |
| (Full Access)|                 |(Time-Limited)|                 | (Azure RBAC) |
+--------------+                 +--------------+                 +--------------+
(Avoid in Prod)                  (Fine-grained)                    (Recommended)
```

1. **Shared Key (Account Keys)**:
   * 512-bit master credentials (Key 1 & Key 2).
   * Grants **full administrative control** to the entire storage account.
   * *Best Practice*: Avoid using Shared Keys in production; disable Shared Key access via Azure Policy or account settings (`--allow-shared-key-access false`).

2. **Shared Access Signature (SAS)**:
   * Delegated URI granting time-bounded, fine-grained access to specific storage resources without exposing account keys.
   * **Types of SAS**:
     * **User Delegation SAS**: Signed with Entra ID credentials (most secure SAS).
     * **Service SAS**: Signed with Storage Account Key; scopes to Blob/Table/Queue/File.
     * **Account SAS**: Signed with Storage Account Key; delegates access to any service-level API.

3. **Microsoft Entra ID (Azure RBAC)**:
   * Standard identity-based access model using OAuth 2.0.
   * Supports fine-grained access control at subscription, RG, storage account, container, or blob level without hardcoded secrets.

---

## 4. Azure Data Lake Storage Gen2 (ADLS Gen2)

**ADLS Gen2** is not a separate service; it is a set of capabilities built directly on top of **Azure Blob Storage (GPv2)** by enabling **Hierarchical Namespace (HNS)**.

### 4.1 Hierarchical Namespace (HNS) vs Flat Namespace

```
FLAT NAMESPACE (Standard Blob)              HIERARCHICAL NAMESPACE (ADLS Gen2)
------------------------------              ---------------------------------
Container: rawdata                          Container: rawdata
│                                           │
├── dir1/dir2/file1.csv (virtual path)      ├── dir1/ (real directory object)
└── dir1/dir2/file2.csv                         └── dir2/ (real directory object)
                                                    ├── file1.csv
                                                    └── file2.csv
```

| Metric / Operation | Flat Namespace (Standard Blob) | Hierarchical Namespace (ADLS Gen2) |
| :--- | :--- | :--- |
| **Directory Representation** | Virtual (simulated via slashes `/` in blob object names). | **Real directory objects** maintaining physical hierarchy. |
| **Rename / Move Directory** | **O(N) - Slow & Expensive**: Reads, copies, and deletes every child blob individually. | **O(1) - Atomic & Instant**: Single metadata operation updating directory pointer. |
| **Delete Directory** | Deletes child blobs one by one. | **Atomic Delete**: Single metadata operation deleting directory tree. |
| **Access Control** | Container or Blob level RBAC. | **POSIX-like ACLs** at directory & file levels + Azure RBAC. |
| **Analytics Engine Optimization**| Slow directory scans (list operations expensive). | **Fast directory scans** (essential for Spark, Databricks, Hive, Trino). |

---

### 4.2 ADLS Gen2 Access Control Model (RBAC + POSIX ACLs)

ADLS Gen2 evaluates authorization using a dual-layer approach: **Azure RBAC** first, followed by **POSIX Access Control Lists (ACLs)**.

```
                                    Incoming Request
                                           |
                                 +---------v---------+
                                 |  Azure RBAC Check |
                                 +---------+---------+
                                           |
                    +----------------------+----------------------+
                    |                                             |
            RBAC Grants Access?                           RBAC Undecided / No RBAC?
                    |                                             |
            +-------v-------+                             +-------v-------+
            |  ACCESS       |                             | Check POSIX   |
            |  GRANTED      |                             |    ACLs       |
            +---------------+                             +-------+-------+
                                                                  |
                                           +----------------------+----------------------+
                                           |                                             |
                                   ACL Grants Access?                            ACL Denies Access?
                                           |                                             |
                                   +-------v-------+                             +-------v-------+
                                   |  ACCESS       |                             |  ACCESS       |
                                   |  GRANTED      |                             |  DENIED       |
                                   +---------------+                             +---------------+
```

#### Dual Layer Authorization Breakdown:
1. **Azure RBAC Layer**:
   * Evaluated at Container level or above.
   * If a user has `Storage Blob Data Contributor` at the Container scope, **RBAC grants full access**, and POSIX ACLs are **bypassed completely**.
2. **POSIX ACLs Layer**:
   * Evaluated when RBAC does **not** explicitly grant data access.
   * Applied at individual Directory or File scope.

#### POSIX ACL Types:
* **Access ACLs**: Control access to a specific object (file or directory).
* **Default ACLs**: Configured on a directory to automatically propagate Access ACLs to newly created child items.

#### ACL Permissions Structure:
```
  Owner (u)    :  rwx  (Read, Write, Execute)
  Group (g)    :  r-x  (Read, Execute)
  Other (o)    :  ---  (No access)
  Mask  (m)    :  rwx  (Maximum allowable permission mask)
```
* **Read (r)**: Read file contents / List directory contents.
* **Write (w)**: Write or create files / Create or delete items in directory.
* **Execute (x)**: Required to traverse a directory hierarchy. (Without `x` on parent directory, user cannot reach child subdirectories!).

> [!WARNING]
> **Traversal Requirement**: To access `/raw/sales/2026/data.parquet` via ACLs, the identity MUST have **Execute (x)** permission on the container root `/`, directory `/raw`, `/raw/sales`, and `/raw/sales/2026`.

---

### 4.3 Drivers & Endpoints (ABFSS Driver)

ADLS Gen2 exposes two distinct REST endpoints:

* **Blob Endpoint**: `https://<account_name>.blob.core.windows.net` (Used by standard Blob tools).
* **DFS Endpoint**: `https://<account_name>.dfs.core.windows.net` (Data Lake Storage REST API used by analytics engines).

#### ABFSS (Azure Blob File System Driver):
The secure Hadoop-compatible driver used by Databricks, Apache Spark, and HDInsight to interact with ADLS Gen2.

```
URI Scheme: abfss://<container-name>@<account-name>.dfs.core.windows.net/<path>/<file>
```

#### Example Spark ABFSS Configuration (Databricks / PySpark):
```python
# Accessing ADLS Gen2 using Service Principal in PySpark
spark.conf.set("fs.azure.account.auth.type.<account_name>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<account_name>.dfs.core.windows.net", "org.apache.hadoop.fs.azureblob.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<account_name>.dfs.core.windows.net", "<client-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<account_name>.dfs.core.windows.net", "<client-secret>")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<account_name>.dfs.core.windows.net", "https://login.microsoftonline.com/<tenant-id>/oauth2/token")

# Reading Delta / Parquet data
df = spark.read.format("parquet").load("abfss://silver@stadlsanalytics01.dfs.core.windows.net/sales/2026/")
```

---

### 4.4 Data Lake Medallion Architecture (Storage Layout Pattern)

```
+-----------------------------------------------------------------------------------+
|                        ADLS Gen2 Container Organization                            |
+-----------------------------------------------------------------------------------+
|                                                                                   |
|  +-------------------+      +-------------------+      +-----------------------+  |
|  |  Bronze (Raw)     | ---> |  Silver (Clean)   | ---> |   Gold (Curated)      |  |
|  |                   |      |                   |      |                       |  |
|  | - Append-only     |      | - Filtered &      |      | - Business aggregates |  |
|  | - Native formats  |      |   standardized    |      | - Star Schema /       |  |
|  |   (CSV, JSON)     |      | - Delta / Parquet |      |   Data Marts          |  |
|  +-------------------+      +-------------------+      +-----------------------+  |
+-----------------------------------------------------------------------------------+
```

---

## 5. Identity & Access Management (IAM) & RBAC

Azure IAM relies on **Microsoft Entra ID** and **Azure Role-Based Access Control (Azure RBAC)** to enforce authentication and authorization.

### 5.1 Security Principals (Identities)

A Security Principal is an entity that requests access to Azure resources.

```
                              Security Principals
                                       |
       +-------------------------------+-------------------------------+
       |                               |                               |
+------v-------+                +------v-------+                +------v-------+
|  User Account|                | Security     |                | Application  |
| (Human User) |                | Group        |                | Identities   |
+--------------+                +--------------+                +------+-------+
                                                                       |
                                                +----------------------+----------------------+
                                                |                                             |
                                        +-------v-------+                             +-------v-------+
                                        | Service       |                             | Managed       |
                                        | Principal     |                             | Identity      |
                                        +---------------+                             +---------------+
```

1. **User**: An individual human account in Entra ID (e.g., `user@company.com`).
2. **Group**: A logical collection of users or other security principals. (Best Practice: Assign roles to Groups, not individual Users).
3. **Service Principal (SPN)**: An application identity created for automated applications, CI/CD pipelines, or background tools to access Azure resources. Uses Application ID + Client Secret / Certificate.
4. **Managed Identity**: Automatically managed service account in Entra ID for Azure resources. **No secrets or credentials to manage or rotate in code!**

#### Managed Identity Types:

| Feature | System-Assigned Managed Identity | User-Assigned Managed Identity |
| :--- | :--- | :--- |
| **Creation** | Created directly on an Azure resource (e.g., ADF instance). | Created as a standalone independent Azure resource. |
| **Lifecycle** | Tied strictly to the lifecycle of the host resource. (Deleting host deletes identity). | Independent lifecycle; can be shared across multiple resources. |
| **Usage Pattern** | 1-to-1 relationship with resource. | 1-to-Many relationship across multiple Azure resources. |

---

### 5.2 Azure RBAC Formula

Azure RBAC authorization works by defining a **Role Assignment**.

$$\text{Role Assignment} = \text{Security Principal} + \text{Role Definition} + \text{Scope}$$

```
  SECURITY PRINCIPAL                 ROLE DEFINITION                     SCOPE
 +------------------+             +------------------+             +---------------+
 |  ADF Managed     |     +       | Storage Blob Data|     +       | Container:    |
 |  Identity        |             | Contributor      |             | /rawdata      |
 +------------------+             +------------------+             +---------------+
```

### 5.3 Hierarchy of Scopes

Role assignments are inherited downwards from higher management levels:

```
  Scope Hierarchy                      Inheritance
  ---------------                      -----------
  1. Management Group     --------->   Inherited by all Subscriptions
  2. Subscription         --------->   Inherited by all Resource Groups
  3. Resource Group       --------->   Inherited by all Resources
  4. Resource             --------->   Inherited by all Child Objects (e.g., Containers)
  5. Child Resource       --------->   Applies only to specified Container / Blob
```

---

### 5.4 Built-In Roles Reference

#### A. Core Governance Roles (Control Plane)

> [!CAUTION]
> Control Plane roles (`Owner`, `Contributor`, `Reader`) control Azure Resource Management (ARM) operations (e.g., creating resources, updating account settings), but do **NOT** grant data plane access to read/write blobs unless explicitly granted by data roles.

| Role Name | Scope Capability | Key Permissions |
| :--- | :--- | :--- |
| **Owner** | Control Plane | Full access to all resources + ability to grant/revoke RBAC roles to others. |
| **Contributor** | Control Plane | Full access to manage resources, but **cannot assign RBAC roles**. |
| **Reader** | Control Plane | View metadata and configuration of resources. Cannot modify anything. |

#### B. Storage Data Roles (Data Plane - Vital for Data Engineers)

| Role Name | Scope Capability | Data Access Level |
| :--- | :--- | :--- |
| **Storage Blob Data Owner** | Data Plane + POSIX | Full access to Blob containers/data + capability to manage POSIX ACLs in ADLS Gen2. |
| **Storage Blob Data Contributor** | Data Plane | Read, Write, Delete objects in Blob storage and ADLS Gen2. |
| **Storage Blob Data Reader** | Data Plane | Read and list objects in Blob storage and ADLS Gen2. |
| **Storage Queue Data Contributor**| Data Plane | Read, write, and delete messages in Azure Storage Queues. |

---

### 5.5 Assigning RBAC Roles via Azure CLI

#### Granting ADF Access to ADLS Gen2:
```bash
# Assign Storage Blob Data Contributor to ADF System-Assigned Identity on ADLS Gen2 Storage Account
az role assignment create \
  --assignee-object-id "<adf-principal-id>" \
  --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/<sub-id>/resourceGroups/rg-analytics-prod-eastus-001/providers/Microsoft.Storage/storageAccounts/stadlsanalytics01"
```

---

## 6. Data Engineering Best Practices & Architecture Patterns

### 6.1 Security Best Practices
1. **Disable Public Access**: Restrict Storage Accounts and Key Vaults to **Private Endpoints (VNet)**.
2. **Disable Shared Key Access**: Force all authentication to use Entra ID (`--allow-shared-key-access false`).
3. **Use Managed Identities**: Prefer Managed Identities over Service Principals to eliminate secret rotation overhead.
4. **Least Privilege Principle**: Assign RBAC roles at the Container or Resource level rather than Subscription level.

### 6.2 Storage Optimization & Cost Control
1. **Automate Lifecycle Management**: Define policies to auto-transition raw files from Hot $\rightarrow$ Cool (30d) $\rightarrow$ Archive (90d).
2. **Use File Formats Built for Analytics**: Store clean lake data in **Parquet** or **Delta Lake** (columnar, compressed, pushdown predicates).
3. **Partition Wisely**: Partition data by standard date hierarchies (`year=2026/month=08/day=08/`) to prevent over-partitioning (avoid small files problem).

---

## 7. Azure Data Lake Storage Gen2 Concepts (In Simple Terms)

### 7.1 Simple Real-World Analogy: Flat vs Hierarchical Namespace

Imagine storing 100,000 files for your company's finance department:

```
FLAT NAMESPACE (Standard Blob Storage)       HIERARCHICAL NAMESPACE (ADLS Gen2)
--------------------------------------       ----------------------------------
A single giant warehouse room filled with    A traditional office filing cabinet with
boxes tagged:                                real nested file folders:
  "finance/2026/Q1/invoice_001.pdf"            📁 finance
  "finance/2026/Q1/invoice_002.pdf"             └── 📁 2026
                                                    └── 📁 Q1
                                                        ├── 📄 invoice_001.pdf
                                                        └── 📄 invoice_002.pdf
```

* **Renaming a directory in Flat Namespace**: To rename directory `finance` to `accounting`, Azure must read, copy, and delete all 100,000 blobs one-by-one ($O(N)$ operation — takes hours!).
* **Renaming a directory in ADLS Gen2**: Azure changes **one pointer in metadata** ($O(1)$ operation — instant!).

> [!IMPORTANT]
> **Why Apache Spark & Databricks REQUIRE ADLS Gen2**: When Spark jobs finish writing data, Spark writes temporary files to `_temporary/` and then renames `_temporary` to `output/`. On flat blob storage, this directory rename step causes Spark jobs to hang for hours. On ADLS Gen2, it is instantaneous.

---

### 7.2 ADLS Gen2 Access Control: RBAC + POSIX ACLs

#### Simple Rule of Thumb for Security:
1. **Azure RBAC**: Use for coarse-grained permissions (e.g., Give the Data Engineering Team `Storage Blob Data Contributor` on the entire storage account or container).
2. **POSIX ACLs**: Use for fine-grained subfolder permissions (e.g., Allow the Finance Team read access *only* to `/raw/finance/` folder and deny access to `/raw/hr/`).

#### Directory Traversal Requirement for POSIX ACLs:
To read `/raw/finance/2026/report.parquet`, an identity needs **Execute (x)** permission on every parent directory in the path:
* `Execute (x)` on `/` (container root)
* `Execute (x)` on `/raw`
* `Execute (x)` on `/raw/finance`
* `Read (r)` on `/raw/finance/2026/report.parquet`

---

### 7.3 ADLS Gen2 Medallion Architecture Example

```
      RAW DATA                      CLEAN DATA                    BUSINESS AGGREGATES
  (CSV, JSON, Logs)               (Delta / Parquet)                (Star Schema / Marts)
          │                               │                                 │
   +------v------+                 +------v------+                   +------v------+
   |   Bronze    |  ───────>       |   Silver    |    ───────>       |    Gold     |
   |  Container  |                 |  Container  |                   |  Container  |
   +-------------+                 +-------------+                   +-------------+
```

* **Bronze (Raw Lake)**: Exact copy of raw source data (append-only, immutable). Example: `abfss://bronze@lake.dfs.core.windows.net/sales/2026/08/09/raw_orders.json`
* **Silver (Cleaned Lake)**: Standardized, deduplicated, cleansed Delta/Parquet data. Example: `abfss://silver@lake.dfs.core.windows.net/orders/`
* **Gold (Business Layer)**: Aggregated data ready for Power BI reporting and business users. Example: `abfss://gold@lake.dfs.core.windows.net/daily_revenue_by_region/`

---

## 8. Azure Data Factory (ADF) Basics

### 8.1 What is Azure Data Factory (ADF)?

**Azure Data Factory (ADF)** is Microsoft's cloud-based **ETL / ELT data integration and orchestration service**.

> [!NOTE]
> **Analogy**: ADF is like a **Factory Dispatcher / Assembly Line Manager**. ADF **does not store data** itself. Instead, it instructs source systems to read data, moves data through pipelines, commands compute engines (like Databricks or Snowflake) to transform data, and loads the output into target data stores.

```
+-----------------------------------------------------------------------------------+
|                        Azure Data Factory (ADF) Pipeline                           |
+-----------------------------------------------------------------------------------+
|                                                                                   |
|  +-------------------+      +-------------------+      +-----------------------+  |
|  |  1. Copy Activity | ---> | 2. Databricks     | ---> | 3. Stored Procedure   |  |
|  |  (Bronze -> Silver|      |    Notebook       |      |    (Update SQL Mart)  |  |
|  +---------+---------+      +---------+---------+      +-----------+-----------+  |
|            |                          |                            |              |
+------------|--------------------------|----------------------------|--------------+
             |                          |                            |
      +------v------+            +------v------+              +------v------+
      | ADLS Gen2   |            | Azure       |              | Azure SQL   |
      | Storage     |            | Databricks  |              | Database    |
      +-------------+            +-------------+              +-------------+
```

---

### 8.2 The 5 Core Concepts of ADF

To build data integration solutions in ADF, you must understand its 5 core building blocks:

```
                          +-------------------------+
                          |        Pipeline         |
                          | (Workflow Container)    |
                          +------------+------------+
                                       |
                                       v
                          +-------------------------+
                          |       Activities        |
                          | (Copy, Notebook, SQL)   |
                          +------------+------------+
                                       |
                                       v
                          +-------------------------+
                          |        Datasets         |
                          | (File / Table Schemas)  |
                          +------------+------------+
                                       |
                                       v
                          +-------------------------+
                          |     Linked Services     |
                          |  (Connection Strings)   |
                          +------------+------------+
                                       |
                                       v
                          +-------------------------+
                          |   Integration Runtime   |
                          |    (Compute Engine)     |
                          +-------------------------+
```

| Component | Simple Description | Real-World Analogy |
| :--- | :--- | :--- |
| **1. Pipeline** | A logical grouping of activities that perform a complete task. | The master **factory assembly line workflow recipe**. |
| **2. Activity** | A single execution step inside a pipeline (e.g., Copy Data, Execute Notebook, Lookup, If Condition, ForEach). | A **specific machine or worker** on the assembly line. |
| **3. Dataset** | Points to the exact data structure or location (e.g., CSV file path in ADLS, Table name in SQL). | The **blueprint or spec sheet** of raw materials/products. |
| **4. Linked Service** | Defines the connection information (URL, credentials) to external sources/targets. | The **security key or passcard** used to access a building. |
| **5. Integration Runtime (IR)** | The compute infrastructure that executes activities and dispatches data movement. | The **engine power / electricity** running the factory. |

---

### 8.3 Integration Runtime (IR) Types Explained Simply

The **Integration Runtime (IR)** is the compute engine of ADF. There are 3 types of IR depending on where your data lives:

```
                            Integration Runtime (IR)
                                       |
       +-------------------------------+-------------------------------+
       |                               |                               |
+------v-------+                +------v-------+                +------v-------+
|   Azure IR   |                | Self-Hosted  |                | Managed VNet |
| (AutoResolve)|                |     (SHIR)   |                |      IR      |
+--------------+                +--------------+                +--------------+
Public Cloud to                 On-Premises / Private            Private Network
Public Cloud                    Corporate Network                Managed Security
```

1. **Azure Integration Runtime (AutoResolve)**:
   * Fully managed by Microsoft in the public cloud.
   * Best for connecting two public cloud resources (e.g., copying data from Blob Storage to Azure SQL DB over public endpoints).
2. **Self-Hosted Integration Runtime (SHIR)**:
   * Software agent installed on an on-premises VM or local server inside your corporate network.
   * **Crucial for Hybrid Scenarios**: Connects ADF in Azure safely to internal on-premises SQL Servers, Oracle, or SAP databases behind company firewalls without opening inbound public firewall ports.
3. **Azure Managed VNet Integration Runtime**:
   * Microsoft provisions dedicated compute isolated inside a private Managed Virtual Network.
   * Uses **Managed Private Endpoints** to connect securely to Azure Storage, Key Vault, or SQL Databases over private IP addresses.

---

### 8.4 ADF Triggers

Triggers determine **WHEN** an ADF pipeline starts executing:

* **Schedule Trigger**: Fires at a fixed calendar time (e.g., Every day at 02:00 AM UTC).
* **Tumbling Window Trigger**: Fires at periodic continuous time intervals (e.g., Every 1 hour). Supports historical backfilling, retries, and pipeline dependency chaining.
* **Storage Event Trigger**: Fires in real-time when a file event occurs in ADLS Gen2 (e.g., When `sales_data.csv` is uploaded to `bronze` container).
* **Custom Event Trigger**: Fires based on Azure Event Grid custom messages.

---

## 9. Azure Key Vault (AKV) & Secret Management

### 9.1 What is Azure Key Vault?

**Azure Key Vault** is a centralized cloud service for securely storing and managing sensitive application secrets, cryptographic keys, and SSL certificates.

> [!TIP]
> **Golden Rule of Cloud Security**: **NEVER hardcode passwords**, connection strings, API keys, or access tokens in source code, scripts, or ADF pipelines. Store them in Key Vault and retrieve them dynamically at runtime!

```
                               +-----------------------------+
                               |     Azure Key Vault (AKV)   |
                               +--------------+--------------+
                                              |
                     +------------------------+------------------------+
                     |                        |                        |
             +-------v-------+        +-------v-------+        +-------v-------+
             |    Secrets    |        |     Keys      |        |  Certificates |
             +---------------+        +---------------+        +---------------+
             | DB Passwords  |        | RSA / ECC     |        | SSL / TLS     |
             | API Tokens    |        | Customer-     |        | Digital       |
             | Connection    |        | Managed Keys  |        | Domain        |
             | Strings       |        | (Encryption)  |        | Certificates  |
             +---------------+        +---------------+        +---------------+
```

---

### 9.2 Key Vault Access Control Models

Key Vault supports two permission models:

1. **Azure RBAC for Key Vault (Recommended)**:
   * Grant fine-grained Azure Entra ID roles:
     * `Key Vault Secrets User`: Read secret values (ideal for ADF / App identities).
     * `Key Vault Secrets Officer`: Read, write, and manage secret lifecycle.
     * `Key Vault Administrator`: Full management of vault objects and permissions.
2. **Vault Access Policies (Legacy)**:
   * Simple permission table configured directly on the vault (e.g., Grant ADF `Get` and `List` permissions on Secrets).

---

### 9.3 Real-World Integration Example: ADF Fetching Credentials from Key Vault

Instead of saving a database password inside an ADF Linked Service, ADF uses its **Managed Identity** to fetch the secret from Key Vault dynamically at runtime:

```
 +------------------+     1. Request Secret Token     +--------------------+
 | Azure Data       | ──────────────────────────────> | Azure Key Vault    |
 | Factory (ADF)    | <────────────────────────────── | (kv-prod-eastus)   |
 +--------+---------+     2. Return SQL Password      +--------------------+
          |
          | 3. Connect using Secret
          v
 +------------------+
 | Azure SQL        |
 | Database         |
 +------------------+
```

#### ADF Linked Service JSON configuration referencing Key Vault:
```json
{
  "name": "ls_azure_sql_db",
  "properties": {
    "type": "AzureSqlDatabase",
    "typeProperties": {
      "connectionString": "Server=tcp:sql-server-prod.database.windows.net;Database=SalesDB;User ID=sqladmin;",
      "password": {
        "type": "AzureKeyVaultSecret",
        "store": {
          "referenceName": "ls_azure_key_vault",
          "type": "LinkedServiceReference"
        },
        "secretName": "sql-db-admin-password"
      }
    }
  }
}
```

---

## 10. Azure Networking Basics for Data Engineering

### 10.1 Real-World Analogy: Why Network Security Matters

* **Public Endpoint Access (Default)**: Your Storage Account or Key Vault has a public IP address on the internet. Anyone on the internet can attempt to scan or attack your login endpoint.
* **Virtual Network (VNet) + Private Endpoints**: You build a secure, fenced gated community for your cloud infrastructure. Your storage account lives inside this gated fence with a **Private IP address** (`10.0.1.5`). Public internet access is completely blocked!

```
                                    PUBLIC INTERNET
                                          │
                                     ❌ BLOCKED (Public Access Disabled)
                                          │
    +-------------------------------------v-------------------------------------+
    | Azure Virtual Network (VNet: 10.0.0.0/16)                                 |
    |                                                                           |
    |  +----------------------------------+   +------------------------------+  |
    |  | Subnet-Compute (10.0.1.0/24)    |   | Subnet-PrivateEndpoints      |  |
    |  |                                  |   | (10.0.2.0/24)                |  |
    |  |  +----------------------------+  |   |  +------------------------+  |  |
    |  |  | ADF Managed VNet IR        |  |   |  | Private Endpoint       |  |  |
    |  |  | (Compute Engine)           |  |   |  | (IP: 10.0.2.5)          |  |  |
    |  |  +--------------+-------------+  |   |  +-----------+------------+  |  |
    |  +-----------------|----------------+   +--------------|---------------+  |
    |                    |                                   |                  |
    |                    +────── Private Traffic (Private Link) ──────+         |
    |                                                        |                  |
    |                                                        v                  |
    |                                            +------------------------+     |
    |                                            | ADLS Gen2 Storage      |     |
    |                                            | stadlsanalytics01      |     |
    |                                            +------------------------+     |
    +---------------------------------------------------------------------------+
```

---

### 10.2 Core Networking Building Blocks

#### 1. Virtual Network (VNet)
An isolated, private network in the Azure cloud (defined using CIDR blocks, e.g., `10.0.0.0/16`). Allows Azure resources (VMs, Databricks clusters, ADF compute) to communicate securely with each other.

#### 2. Subnets
Sub-divisions of a VNet used to organize and isolate different layers of an application (e.g., `10.0.1.0/24` for compute nodes, `10.0.2.0/24` for private endpoints).

#### 3. Network Security Group (NSG)
A virtual stateful firewall that controls inbound and outbound network traffic to subnets or individual network interfaces based on priority rules (Allow/Deny source IP, port, and protocol).

---

### 10.3 Public Access vs Service Endpoints vs Private Endpoints

| Networking Approach | Public IP Address Active? | Traffic Route | Security Level | Best Practice Recommendation |
| :--- | :--- | :--- | :--- | :--- |
| **Public Endpoint** | **Yes** (Exposed to internet) | Over Public Internet | ⚠️ Low (Relies strictly on authentication keys/RBAC) | **Avoid in production**. |
| **Service Endpoints** | **Yes** (Public IP still exists) | Over Azure Backbone Network | 🟡 Medium (Firewall restricts access to specific Subnet) | Acceptable for basic workloads. |
| **Private Endpoint & Private Link** | **No** (Public Access Disabled!) | Direct Private IP inside your VNet | 🔒 **High (Enterprise Standard)** | **Mandatory Enterprise Security Standard**. |

---

## 11. End-to-End Secure Data Engineering Architecture

Here is how all these core components (ADLS Gen2, ADF, Key Vault, Networking, and IAM) fit together in a production-ready enterprise architecture:

```
+---------------------------------------------------------------------------------------------------+
|                              AZURE ENTERPRISE VNET (10.0.0.0/16)                                  |
|                                                                                                   |
|   +---------------------------------------+               +-----------------------------------+   |
|   | Subnet-ADF (10.0.1.0/24)              |               | Subnet-PrivateEndpoints           |   |
|   |                                       |               | (10.0.2.0/24)                     |   |
|   |  +---------------------------------+  |               |                                   |   |
|   |  | Azure Data Factory (ADF)        |  |               |  +-----------------------------+  |   |
|   |  | Managed Identity Enabled        |  |               |  | Key Vault Private Endpoint   |  |   |
|   |  +----------------+----------------+  |               |  | (10.0.2.10)                 |  |   |
|   +-------------------|-------------------+               |  +--------------+--------------+  |   |
|                       |                                   |                 ^                 |   |
|                       | 1. Read Secret (Managed Identity) |                 |                 |   |
|                       +───────────────────────────────────+────────────────-+                 |   |
|                       |                                                                       |   |
|                       | 2. Private Data Transfer (Private Link)                               |   |
|                       v                                                                       |   |
|   +---------------------------------------------------------------------------------------+   |
|   | ADLS Gen2 Storage Account (Public Access: DISABLED)                                  |   |
|   | Private Endpoint IP: 10.0.2.20                                                        |   |
|   |                                                                                       |   |
|   |  +-----------------------+   +-----------------------+   +-----------------------+    |   |
|   |  | Bronze Container      |   | Silver Container      |   | Gold Container        |    |   |
|   |  | (Raw Data)            |   | (Cleaned Delta)       |   | (Aggregated Marts)    |    |   |
|   |  +-----------------------+   +-----------------------+   +-----------------------+    |   |
|   +---------------------------------------------------------------------------------------+   |
+---------------------------------------------------------------------------------------------------+
```

### Flow of Data & Execution:
1. **Trigger**: ADF Storage Event Trigger fires when raw data arrives.
2. **Key Vault Lookup**: ADF retrieves database credentials from Key Vault using its **System-Assigned Managed Identity** via Private Endpoint (`10.0.2.10`).
3. **Data Ingestion**: ADF Copy Activity moves data into **ADLS Gen2 Bronze Container** over Private Endpoint (`10.0.2.20`).
4. **Data Transformation**: ADF triggers a Databricks Notebook to process Bronze JSON into **Silver Delta Lake** tables.
5. **Access Control**: Fine-grained **POSIX ACLs** restrict raw Bronze folder access to Data Engineers only, while Silver/Gold is accessible by Data Analysts.
