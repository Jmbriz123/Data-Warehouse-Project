# Data Catalog: Gold Layer (Analytical)

## 1. Overview
The **Gold Layer** is the consumption-ready tier of the data warehouse. It is modeled as a **Star Schema** to provide a performant and intuitive experience for end-users and BI tools.

### Global Standards
* **Programming Language Used:** PostgreSQL
* **Temporal Tracking:** All timestamps use `TIMESTAMPTZ` to ensure UTC consistency.
* **Audit Columns:** * `_gold_processed_at`: Ingestion wall-clock time.
    * `_source_system`: Origin system (e.g., 'CRM_PROD', 'ERP_SOUTH').

---

## 2. Dimensional Models

### 2.1 `gold.dim_customers`
**Description:** Contains master customer records. Implements Type 1 SCD (overwrites).  
**Grain:** 1 row per unique `customer_id`.

| Column Name | Data Type | Constraints | Description                         |
| :--- | :--- | :--- |:------------------------------------|
| **customer_key** | BIGINT | **PK** | Generated surrogate key.            |
| **customer_id** | INTEGER | UK | Natural key from source system.     |
| **customer_number** | TEXT | - | Business-facing customer reference. |
| **first_name** | TEXT | - | First name of the customer          |
| **last_name** | TEXT | - | Last name of the customer           |
| **country** | TEXT | - | Normalized country name.            |
| **marital_status** | TEXT | - | {Married, Single, etc.}             |
| **gender** | TEXT | - | {Male, Female, N/A}                 |
| **birthdate** | DATE | - | Format: YYYY-MM-DD.                 |
| **created_date** | DATE | - | Initial registration date.          |
| **_gold_processed_at**| TIMESTAMPTZ | - | Metadata: Record update time.       |
| **_source_system** | TEXT | - | Metadata: Originating source.       |

---

### 2.2 `gold.dim_products`
**Description:** Product catalog including classification hierarchies.  
**Grain:** 1 row per unique `product_id`.

| Column Name | Data Type   | Constraints | Description |
| :--- |:------------| :--- | :--- |
| **product_key** | BIGINT      | **PK** | Generated surrogate key. |
| **product_id** | INTEGER     | UK | Source system identifier. |
| **product_number** | TEXT        | - | SKU / Model number. |
| **product_name** | TEXT        | - | Full product description. |
| **category_id** | TEXT        | - | Product category code. |
| **category** | TEXT        | - | High-level group (e.g., 'Bikes'). |
| **subcategory** | TEXT        | - | Detailed group (e.g., 'Road Bikes'). |
| **maintenance** | BOOLEAN     | - | Requirement for post-sale service. |
| **cost** | INTEGER     | - | Base manufacturing/purchase cost. |
| **product_line** | TEXT        | - | Brand/Series classification. |
| **start_date** | DATE        | - | Catalog entry date. |
| **_gold_processed_at**| TIMESTAMPTZ | - | Metadata: Record update time. |
| **_source_system** | TEXT        | - | Metadata: Originating source. |

---

## 3. Fact Models

### 3.1 `gold.fact_sales`
**Description:** Atomic sales transactions.  
**Grain:** 1 row per Line Item per Order.  
**Partitioning:** List/Range partitioned by `order_date` (Monthly).

| Column Name | Data Type     | Constraints | Description |
| :--- |:--------------|:------------| :--- |
| **order_number** | TEXT          | **UK**      | Transaction ID (e.g., 'SO12345'). |
| **product_key** | BIGINT        | **FK**      | Joins to `gold.dim_products`. |
| **customer_key** | BIGINT        | **FK**      | Joins to `gold.dim_customers`. |
| **order_date** | DATE          | -           | Primary event date (Partition Key). |
| **shipping_date** | DATE          | -           | Date of dispatch. |
| **due_date** | DATE          | -           | Expected payment date. |
| **sales_amount** | INTEGER       | -           | Net revenue for this line item. |
| **quantity** | INTEGER       | -           | Unit count sold. |
| **price** | NUMERIC(18,2) | -           | Unit price at transaction time. |
| **_gold_processed_at**| TIMESTAMPTZ   | -           | Metadata: Processing timestamp. |
| **_source_system** | TEXT          | -           | Metadata: Originating source. |