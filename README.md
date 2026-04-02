# 🛢️ Data Warehouse and Analytics Project (PostgreSQL)

A structured Data Warehouse built in **PostgreSQL**, following the **Medallion Architecture**. This project focuses on the **ELT** (Extract, Load, Transform) pattern, where raw data is ingested into a Bronze layer and processed entirely via SQL.

---

## 🏗️ Architecture Overview
The project is divided into three logical layers (schemas) to ensure data integrity and auditability:

1.  **Bronze (Raw) 🟢 [COMPLETED]:** 1:1 copies of source datasets. Includes audit metadata (`_loaded_at`) and uses automated stored procedures for high-performance ingestion.
2.  **Silver (Clean) 🟡 [IN PROGRESS]:** The "Source of Truth." Data is de-duplicated, type-casted to strict formats, and validated against business rules.
3.  **Gold (Curated) ⚪ [PLANNED]:** Aggregated views and tables ready for analytics, such as customer purchase summaries or product performance.

---

## 🚀 Project Setup

### 1. Database Initialization
Before loading data, run the initialization scripts located in `scripts/migrations/` to set up the environment.

1. Connect to your local PostgreSQL instance.
2. Execute `init_database.sql` to create the `dwh_project` database.
3. Execute `001_init_medallion_schemas.sql` to generate the `bronze`, `silver`, and `gold` schemas.
4. Execute `002_init_bronze_crm_tables.sql` and `003_init_bronze_erp_tables.sql` to initialize tables on the bronze layer
### 2. Data Ingestion (Bronze Layer)
The ingestion process is orchestrated via a master execution script. This script calls the stored procedures, passes the necessary local directory paths, and performs a final data validation.

1. Open `scripts/ingestion/run_bronze_load.sql`.
2. Update the `p_source_dir` arguments in the `CALL` statements to match the **absolute paths** of your local `datasets` folders.
3. Execute the script in your SQL tool or via CLI:
   ```bash
   psql -U postgres -d your_db_name -f scripts/ingestion/run_bronze_load.sql
## 📂 Data Dictionary (Bronze Layer)

The following datasets represent the raw ingestion tables within the `bronze` schema.

### 1. Source: CRM (Customer Relationship Management)
| Table Name | Source File | Description |
| :--- | :--- | :--- |
| `crm_cust_info` | `cust_info.csv` | Primary customer profiles and registration dates. |
| `crm_prd_info` | `prd_info.csv` | Product catalog details and unit prices. |
| `crm_sales_details`| `sales_details.csv`| Transaction logs for customer purchase history. |

### 2. Source: ERP (Enterprise Resource Planning)
| Table Name | Source File | Description |
| :--- | :--- | :--- |
| `erp_cust_az12` | `CUST_AZ12.csv` | ERP-specific customer account mapping. |
| `erp_loc_a101` | `LOC_A101.csv` | Location metadata for physical stores. |
| `erp_px_cat_g1v2` | `PX_CAT_G1V2.csv` | Global product category hierarchy. |

---

## 🛠️ Engineering Principles
- **Idempotency:** All ingestion scripts use `TRUNCATE` to ensure a clean "wipe-and-reload" state, allowing them to be executed repeatedly without duplicating data.
- **Schema Isolation:** Logical separation of concerns using PostgreSQL schemas (`bronze`, `silver`, `gold`).
- **Zero-Footprint Data:** Large datasets are managed locally; only logic (SQL) and documentation are versioned.