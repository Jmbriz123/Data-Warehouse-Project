# 🛢️Data Warehouse and Analytics Project (PostgreSQL)

A structured Data Warehouse built in **PostgreSQL**, following the **Medallion Architecture**. This project focuses on the **ELT** (Extract, Load, Transform) pattern, where raw data is ingested into a Bronze layer and processed entirely via SQL.

---

## 🏗️ Architecture Overview
The project is divided into three logical layers (schemas) to ensure data integrity and auditability:

1.  **Bronze (Raw):** 1:1 copies of source datasets. Includes audit metadata (`ingested_at`) and uses flexible data types to ensure high ingestion success rates.
2.  **Silver (Clean):** The "Source of Truth." Data is de-duplicated, type-casted to strict formats, and validated against business rules.
3.  **Gold (Curated):** Aggregated views and tables ready for analytics, such as average fuel prices per region or customer purchase summaries.

---

## 🚀 Project Setup

### 1. Database Initialization
Before loading data, run the initialization scripts located in `scripts/migrations/` to set up the environment.

1. Connect to your local PostgreSQL instance.
2. Execute `init_database.sql` to create the `dwh_project` database.
3. Execute `001_init_medallion_schemas.sql` to generate the `bronze`, `silver`, and `gold` schemas.

### 2. Local Data Management
Raw data resides in the `datasets/` directory but is **excluded from Version Control (Git)** via `.gitignore` to prevent repository bloating and data leaks.

- `datasets/source_crm/`: Contains customer and product info.
- `datasets/source_erp/`: Contains location and category metadata.

*Note: Use the `.gitkeep` files to maintain the directory structure when cloning the repository.*

---
## 📂 Data Dictionary (Bronze Layer)

The following datasets represent the raw ingestion tables within the `bronze` schema. All tables include audit columns: `ingested_at` (timestamp) and `source_file` (string).

### 1. Source: CRM (Customer Relationship Management)
| Table Name | Source File | Description |
| :--- | :--- | :--- |
| `cust_info` | `cust_info.csv` | Primary customer profiles, contact info, and registration dates. |
| `prd_info` | `prd_info.csv` | Product catalog details, including categories and unit prices. |
| `sales_details`| `sales_details.csv`| CRM-side transaction logs for customer purchase history. |

### 2. Source: ERP (Enterprise Resource Planning)
| Table Name | Source File | Description |
| :--- | :--- | :--- |
| `cust_az12` | `CUST_AZ12.csv` | ERP-specific customer account mapping and credit status. |
| `loc_a101` | `LOC_A101.csv` | Location metadata for physical stores or gas stations. |
| `px_cat_g1v2` | `PX_CAT_G1V2.csv` | Product category hierarchy and global taxonomy codes. |
---

## 🛠️ Engineering Principles
- **Idempotency:** All DDL scripts use `IF NOT EXISTS` so they can be executed repeatedly without errors.
- **Principle of Least Privilege:** Infrastructure is managed via a dedicated `dwh_admin` role.
- **Zero-Footprint Data:** Large datasets are managed locally; only logic (SQL) and documentation are versioned.