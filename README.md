# 🛢️ Data Warehouse and Analytics Project (PostgreSQL)

A structured Data Warehouse built in **PostgreSQL**, following the **Medallion Architecture**.
This project focuses on the **ELT** (Extract, Load, Transform) pattern, where raw data is
ingested into a Bronze layer and processed entirely via SQL — no external ETL tools, no
orchestration frameworks, just disciplined SQL engineering.

---

## 🏗️ Architecture Overview
<img width="980" height="607" alt="image" src="https://github.com/user-attachments/assets/7a778ab5-cc84-42b2-a6b0-51c4fdd9a012" />

| Layer | Status | Purpose |
| :--- | :--- | :--- |
| 🟢 **Bronze** | COMPLETED | 1:1 copies of source data. No transformations. Audit metadata only. |
| 🟡 **Silver** | COMPLETED | Source of Truth. Deduplicated, typed, validated, and business-rule compliant. |
| 🟠 **Gold** | COMPLETED | Analytics-ready Star Schema materialized views for reporting and dashboards. |

---

## 📂 Repository Structure

```
Data-Warehouse/
│
├── datasets/                          # Raw CSV source files (local only, not versioned)
│
├── docs/                              # Project documentation and diagrams
│
├── scripts/
│   ├── bronze/
│   │   ├── procedures/
│   │   │   ├── load_crm_data.sql      # Stored procedure: Bronze CRM ingestion
│   │   │   └── load_erp_data.sql      # Stored procedure: Bronze ERP ingestion
│   │   ├── tables/                    # Bronze table DDL definitions
│   │   └── run_bronze_load.sql        # Runner: calls all Bronze procedures in order
│   │
│   ├── exploration/
│   │   └── bronze/
│   │       └── 01_explore_data_quality_issues/
│   │           ├── explore_crm_cust_info.sql
│   │           ├── explore_crm_prd_info.sql
│   │           ├── explore_crm_sales_details.sql
│   │           ├── explore_erp_custaz12.sql
│   │           ├── explore_erp_loc_a101.sql
│   │           └── explore_erp_px_cat_g1v2.sql
│   │
│   ├── gold/
│   │   ├── views/
│   │   │   ├── dim_customers.sql      # Customer dimension materialized view
│   │   │   ├── dim_products.sql       # Product dimension materialized view
│   │   │   └── fact_sales.sql         # Sales fact materialized view
│   │   ├── gold_indexes.sql           # Gold layer index definitions (run once on setup)
│   │   └── run_gold_load.sql          # Runner: refreshes all Gold materialized views in order
│   │
│   ├── migrations/
│   │   ├── 001_init_medallion_schemas.sql
│   │   ├── 002_init_bronze_crm_tables.sql
│   │   ├── 003_init_bronze_erp_tables.sql
│   │   ├── 004_init_silver_crm_tables.sql
│   │   ├── 005_init_silver_erp_tables.sql
│   │   ├── 006_alter_crm_prd_table.sql
│   │   └── 007_alter_crm_sales_details_table.sql
│   │
│   └── silver/
│       ├── procedures/
│       │   ├── transform_and_load_crm_cust_info_data.sql
│       │   ├── transform_and_load_crm_prd_info.sql
│       │   ├── transform_and_load_crm_sales_details.sql
│       │   ├── transform_and_load_erp_cust_az12.sql
│       │   ├── transform_and_load_erp_loc_a101.sql
│       │   └── transform_and_load_erp_px_cat_g1v2.sql
│       └── run_silver_load.sql        # Runner: drop indexes → load → recreate indexes
│
├── tests/
│   └── silver/
│       ├── check_data_quality_silver_crm_cust_info.sql
│       ├── check_data_quality_silver_crm_prd_info.sql
│       ├── check_data_quality_silver_crm_sales_details.sql
│       ├── check_data_quality_silver_erp_cust_az12.sql
│       ├── check_data_quality_silver_erp_loc_a101.sql
│       └── check_data_quality_silver_erp_px_cat_g1v2.sql
│
├── init_database.sql                  # Creates the dwh_project database
├── .gitignore
└── README.md
```

---

## 🚀 Project Setup

### 1. Database Initialization
Run the initialization scripts in `scripts/migrations/` in order:

1. Connect to your local PostgreSQL instance.
2. Execute `init_database.sql` — creates the `dwh_project` database.
3. Execute `001_init_medallion_schemas.sql` — generates `bronze`, `silver`, and `gold` schemas.
4. Execute `002_init_bronze_crm_tables.sql` and `003_init_bronze_erp_tables.sql` — initializes Bronze tables.
5. Execute `004_init_silver_crm_tables.sql` and `005_init_silver_erp_tables.sql` — initializes Silver tables.
6. Execute `006_alter_crm_prd_table.sql` and `007_alter_crm_sales_details_table.sql` — applies schema amendments.

### 2. Data Ingestion (Bronze Layer)

1. Open `scripts/bronze/run_bronze_load.sql`.
2. Update the `p_source_dir` arguments to match the **absolute paths** of your local `datasets` folders.
3. Execute:
   ```bash
   psql -U postgres -d dwh_project -f scripts/bronze/run_bronze_load.sql
   ```

### 3. Silver Layer Transformation

1. Open `scripts/silver/run_silver_load.sql`.
2. Execute:
   ```bash
   psql -U postgres -d dwh_project -f scripts/silver/run_silver_load.sql
   ```
   This drops all Silver indexes, calls all six transformation procedures in dependency order
   (dimensions before facts, CRM before ERP), then recreates indexes optimized for the Gold
   build queries.

### 4. Data Quality Checks (Silver Layer)

After the Silver load, run the test scripts to validate the output:

```bash
psql -U postgres -d dwh_project -f tests/silver/check_data_quality_silver_crm_cust_info.sql
psql -U postgres -d dwh_project -f tests/silver/check_data_quality_silver_crm_prd_info.sql
psql -U postgres -d dwh_project -f tests/silver/check_data_quality_silver_crm_sales_details.sql
psql -U postgres -d dwh_project -f tests/silver/check_data_quality_silver_erp_cust_az12.sql
psql -U postgres -d dwh_project -f tests/silver/check_data_quality_silver_erp_loc_a101.sql
psql -U postgres -d dwh_project -f tests/silver/check_data_quality_silver_erp_px_cat_g1v2.sql
```

Each script ends with a **summary scoreboard** — a single aggregated pass/fail count across
all checks. A fully clean Silver layer returns zero failures.

### 5. Gold Layer Setup (First Time Only)

On first deployment, create the materialized views and initialize the Gold indexes:

```bash
# Create the materialized views
psql -U postgres -d dwh_project -f scripts/gold/views/dim_products.sql
psql -U postgres -d dwh_project -f scripts/gold/views/dim_customers.sql
psql -U postgres -d dwh_project -f scripts/gold/views/fact_sales.sql

# Create Gold indexes — run ONCE, indexes persist across refreshes
psql -U postgres -d dwh_project -f scripts/gold/gold_indexes.sql
```

### 6. Gold Layer Refresh (Every Batch Run)

Once Silver passes all quality checks, refresh the Gold materialized views:

```bash
psql -U postgres -d dwh_project -f scripts/gold/run_gold_load.sql
```

This refreshes dimensions first, then the fact table, in strict dependency order.
`REFRESH MATERIALIZED VIEW CONCURRENTLY` is used — analysts can continue querying
Gold without interruption during the refresh window.

---

## ⚡ Performance Optimization — Indexing Strategy

This project implements a **scale-aware indexing strategy** across both the Silver and Gold
layers. The strategy is designed around the access patterns of each layer, not just current
data volume. At the current dataset size the gains are modest — but the architecture ensures
the pipeline degrades gracefully as data grows into the millions of rows.

---

### Silver Layer — Indexes for ETL Performance

Silver indexes exist for one purpose: **accelerating the Gold build queries**. They are not
for analyst access. Every indexed column in Silver corresponds directly to a `JOIN` or `WHERE`
clause in a Gold materialized view build script.

#### Why Drop and Recreate on Every Batch Run

The Silver pipeline follows a strict **drop → load → recreate** pattern:

```
STEP 1: DROP all Silver indexes
STEP 2: CALL transform procedures (TRUNCATE + INSERT inside each)
STEP 3: CREATE all Silver indexes
```

This is the correct pattern for batch pipelines for two reasons:

**Reason 1 — Bulk insert performance.** Maintaining a B-tree index during row-by-row
insertion requires a sorted traversal for every single row — finding the correct position
in the tree, checking for page capacity, and splitting pages when they fill. At scale this
becomes the dominant cost of the load. Dropping indexes before the bulk insert eliminates
this overhead entirely.

**Reason 2 — Index build quality.** `CREATE INDEX` after the load scans the table once,
sorts all keys in memory in a single pass, and writes the B-tree sequentially from left to
right. This produces a perfectly balanced tree with no fragmentation — consistently faster
than the incrementally built tree that results from maintaining an index during insertion.

The `TRUNCATE` inside each procedure resets the table data. The drop-recreate cycle ensures
the index is also rebuilt cleanly on every run.

#### Silver Index Coverage

| Table | Indexed Column | Justifies By |
| :--- | :--- | :--- |
| `silver.crm_prd_info` | `cat_id` | JOIN to `erp_px_cat_g1v2` in `dim_products` build |
| `silver.crm_prd_info` | `prd_key` | JOIN to `crm_sales_details` in `fact_sales` build |
| `silver.crm_prd_info` | `prd_end_dt` | `WHERE prd_end_dt IS NULL` filter in `dim_products` build (SCD Type 2) |
| `silver.erp_px_cat_g1v2` | `id` | JOIN target from `crm_prd_info.cat_id` |
| `silver.crm_cust_info` | `cst_key` | JOIN to `erp_cust_az12` and `erp_loc_a101` in `dim_customers` build |
| `silver.crm_cust_info` | `cst_id` | JOIN to `crm_sales_details` in `fact_sales` build |
| `silver.erp_cust_az12` | `cid` | JOIN target from `crm_cust_info.cst_key` |
| `silver.erp_loc_a101` | `cid` | JOIN target from `crm_cust_info.cst_key` |
| `silver.crm_sales_details` | `sls_prd_key` | JOIN to `crm_prd_info` in `fact_sales` build |
| `silver.crm_sales_details` | `sls_cust_id` | JOIN to `crm_cust_info` in `fact_sales` build |
| `silver.crm_sales_details` | `sls_order_dt` | Incremental load date filter (defensive, for future use) |

> **Design principle:** Silver indexes are specified by reading the Gold build scripts — not
> guessed. Every indexed column maps to an explicit `JOIN ON` or `WHERE` clause. No index
> exists without a documented justification.

---

### Gold Layer — Indexes for Analyst Query Performance

Gold indexes serve a different master: **the analysts and BI tools querying the star schema**.
Unlike Silver, Gold indexes are created once on first setup and survive every `REFRESH` —
PostgreSQL rebuilds them automatically after each materialized view refresh.

#### Why Gold Indexes Are Never Dropped

Gold materialized views are queried continuously by analysts. Dropping indexes would make
the views unavailable for efficient querying at any point — defeating the purpose of the
serving layer. `REFRESH MATERIALIZED VIEW CONCURRENTLY` (used in `run_gold_load.sql`)
requires unique indexes to exist and handles index maintenance internally. No manual
drop-recreate cycle is needed or appropriate here.

#### Unique Indexes — Required for `REFRESH CONCURRENTLY`

`REFRESH MATERIALIZED VIEW CONCURRENTLY` requires at least one unique index per materialized
view. Without it, PostgreSQL cannot perform a non-blocking refresh and will error. These
indexes also enforce the grain of each object:

| Materialized View | Unique Index | Grain Enforced |
| :--- | :--- | :--- |
| `gold.dim_products` | `product_key` | 1 row per active product |
| `gold.dim_customers` | `customer_key` | 1 row per customer |
| `gold.fact_sales` | `(customer_key, product_key, order_number)` | 1 row per line item per order |

The fact table uses a **composite unique index** across all three columns because a customer
can purchase the same product multiple times across different orders — neither FK alone nor
`order_number` alone guarantees uniqueness at the line-item grain.

#### Attribute Indexes — Analyst Drill-Down Performance

These indexes target the columns analysts filter, group, and drill down on in BI tools and
ad-hoc queries. They are chosen based on the known access patterns of a star schema:

| Materialized View | Indexed Column | Access Pattern |
| :--- | :--- | :--- |
| `gold.dim_products` | `category` | GROUP BY / WHERE drill-down |
| `gold.dim_products` | `subcategory` | GROUP BY / WHERE drill-down |
| `gold.dim_products` | `product_line` | GROUP BY / WHERE filter |
| `gold.dim_customers` | `country` | WHERE / GROUP BY geographic filter |
| `gold.dim_customers` | `gender` | WHERE demographic filter |
| `gold.dim_customers` | `marital_status` | WHERE demographic filter |
| `gold.fact_sales` | `product_key` | FK JOIN from fact to dim_products |
| `gold.fact_sales` | `customer_key` | FK JOIN from fact to dim_customers |
| `gold.fact_sales` | `order_date` | WHERE date range filter |
| `gold.fact_sales` | `shipping_date` | WHERE date range filter |

---

### Scalability Rationale

The current dataset sits below the threshold where indexes produce measurable wall-clock
improvements — PostgreSQL's sequential scan is efficient at small volumes. The indexing
strategy is implemented now because:

- **Access patterns are certain.** Star schema query patterns (FK joins, date range filters,
  category drill-downs) are well-understood and will not change as data grows. Indexes placed
  today reflect patterns that will be active at any volume.
- **Retrofitting is disruptive.** Adding indexes to a live multi-million-row Gold layer
  requires `CREATE INDEX CONCURRENTLY` with significant I/O overhead and careful timing.
  Placing them now costs nothing at small volume and eliminates the risk of degraded analyst
  performance during a future migration.
- **The architecture is correct regardless of volume.** A pipeline that handles 60k rows
  with the same structural discipline as one handling 60M rows is a stronger demonstration
  of engineering judgment than one that optimises reactively.

At production scale (10M+ rows on the fact table), the expected impact is:

| Query Pattern | Without Indexes | With Indexes | Estimated Improvement |
| :--- | :--- | :--- | :--- |
| Fact-to-dimension JOIN | Full Seq Scan | Index Scan on FK | ~95% reduction |
| Date range filter on fact | Full Seq Scan | Index Scan on order_date | ~95% reduction |
| Category drill-down | Full Seq Scan on dim | Index Scan on category | ~90% reduction |
| Silver Gold build (JOIN-heavy) | Seq Scan all silver tables | Index Scan on JOIN keys | ~80–95% reduction |

---

## 📋 Data Dictionary

### Source: CRM (Customer Relationship Management)

| Bronze Table | Silver Table | Source File | Description |
| :--- | :--- | :--- | :--- |
| `bronze.crm_cust_info` | `silver.crm_cust_info` | `cust_info.csv` | Customer profiles and registration dates. |
| `bronze.crm_prd_info` | `silver.crm_prd_info` | `prd_info.csv` | Product catalog with category and pricing. |
| `bronze.crm_sales_details` | `silver.crm_sales_details` | `sales_details.csv` | Transaction logs for customer purchases. |

### Source: ERP (Enterprise Resource Planning)

| Bronze Table | Silver Table | Source File | Description |
| :--- | :--- | :--- | :--- |
| `bronze.erp_cust_az12` | `silver.erp_cust_az12` | `CUST_AZ12.csv` | ERP customer demographics — birthdate and gender. |
| `bronze.erp_loc_a101` | `silver.erp_loc_a101` | `LOC_A101.csv` | Customer location and country data. |
| `bronze.erp_px_cat_g1v2` | `silver.erp_px_cat_g1v2` | `PX_CAT_G1V2.csv` | Global product category hierarchy. |

### Gold Layer: Star Schema

| Gold Object | Type | Description |
| :--- | :--- | :--- |
| `gold.dim_customers` | Materialized View | Master customer records, Type 1 SCD. 1 row per customer. |
| `gold.dim_products` | Materialized View | Product catalog with category hierarchy. 1 row per active product. |
| `gold.fact_sales` | Materialized View | Atomic sales transactions. 1 row per line item per order. |

---

## 🧱 Silver Layer — Engineering Deep Dive

The Silver layer is the most technically demanding layer in this project. Raw data from two
separate source systems (CRM and ERP) arrives with inconsistent formats, duplicate records,
broken business rules, and mismatched keys. Every transformation decision in this layer was
**driven by a prior data exploration phase** — nothing was assumed, everything was profiled
first. The exploration scripts live in `scripts/exploration/bronze/01_explore_data_quality_issues/`.

### The Core Challenge: Two Systems That Don't Speak the Same Language

The fundamental problem this layer solves is that CRM and ERP data were never designed to
join together. They use different key formats, different value encodings, and different
conventions for representing the same concept. Understanding the relationships between tables
was itself a significant challenge — messy data obscures structure, and a join that silently
returns zero rows looks identical to a join that was never attempted.

Integration friction discovered during exploration:

- `crm_prd_info.prd_key` uses `_` as a delimiter. `erp_px_cat_g1v2.id` uses `-` for the
  same concept. A direct join returns zero matches with no error raised.
- `erp_cust_az12.cid` carries a `NAS` prefix on some rows that does not exist in
  `crm_cust_info.cst_key`. A direct join silently drops most customer records.
- `erp_loc_a101.cid` contains `-` delimiters that `crm_cust_info.cst_key` does not.
  Same concept, two formats, zero matches without transformation.
- `crm_prd_info.prd_key` encodes two pieces of information — a category reference and a
  product identifier — in a single composite string. This was only discovered by visually
  comparing values side by side against `erp_px_cat_g1v2.id`.

None of these issues surface as errors. They manifest as empty result sets and incorrect
aggregations — which is why the exploration phase was non-negotiable before any transformation
was written.

---

### Table-by-Table Transformation Decisions

#### `silver.crm_cust_info` — Customer Dimension

**Discoveries during exploration:**
- `cst_id` is not unique in the Bronze layer. The same customer appears multiple times,
  representing a history of updates from the CRM system.
- Text columns contain leading and trailing whitespace.
- `cst_marital_status` and `cst_gndr` store abbreviated codes (`S`, `M`, `F`) instead of
  human-readable labels.
- Some records have a NULL `cst_id` — no primary key, no identity, no usable record.

**Transformation decisions and why:**
- **Deduplication via `ROW_NUMBER()`:** Partitioned by `cst_id`, ordered by `cst_create_date DESC`
  and `_loaded_at DESC` as a tie-breaker. This keeps the most recent version of each customer —
  a deliberate choice over simpler but less correct alternatives like `DISTINCT` or `MAX()`.
- **`TRIM()` on all text columns:** Whitespace in name fields causes silent join failures and
  incorrect string comparisons downstream. Stripping it here means Gold and analytics layers
  never have to compensate for it.
- **`CASE WHEN` conforming:** Expanding `'S'` → `'Single'` and `'F'` → `'Female'` at the
  Silver layer means the Gold layer and any BI tool consuming the data never needs to decode
  source abbreviations. Business-readable labels belong in the Source of Truth.
- **NULL `cst_id` rejection:** Records without a primary key cannot be deduplicated or joined.
  They are excluded at the predicate level rather than loaded as orphaned rows.

---

#### `silver.crm_prd_info` — Product Dimension

**Discoveries during exploration:**
- `prd_key` is a **composite key** — it encodes both a category reference and a product
  identifier in a single string (`XX-XX-<product_segment>`). This was discovered by placing
  `prd_key` values next to `erp_px_cat_g1v2.id` values and comparing them visually.
- The category segment uses `-` as a delimiter in the ERP table but `_` in the CRM table —
  a format mismatch that causes silent join failures.
- `prd_end_dt` is sometimes earlier than `prd_start_dt` in the source — the CRM does not
  maintain correct date boundaries for product version history.
- `prd_cost` has NULL values (no negatives found).
- `prd_line` stores abbreviated codes (`M`, `R`, `S`, `T`).

**Transformation decisions and why:**
- **Key decomposition:** `prd_key` is split into `cat_id` (positions 1–5) and `prd_key`
  (position 7 onwards). This was the only way to enable reliable joins to the category table
  and was only possible after understanding the composite key structure through exploration.
- **Delimiter normalisation (`REPLACE('-', '_')`):** The extracted `cat_id` has its `-`
  replaced with `_` to match `erp_px_cat_g1v2.id`. Without this, every category join in
  the Gold layer silently returns zero rows.
- **SCD Type 2 end date derivation via `LEAD()`:** The broken `prd_end_dt` from the source
  is discarded. The correct end date is derived as the day before the next version's start
  date within the same product key. A `NULL` end date signals the currently active version.
  This is the standard Slowly Changing Dimension Type 2 pattern.
- **`COALESCE(prd_cost, 0)`:** Missing cost is treated as zero, making the column
  aggregation-safe downstream without requiring defensive `COALESCE` calls in every Gold query.
- **Malformed key rejection:** Rows where `prd_key` is too short to safely extract both
  segments are excluded and counted before the load begins.

---

#### `silver.crm_sales_details` — Sales Fact Table

**Discoveries during exploration:**
- Date columns are stored as **integers** in `YYYYMMDD` format. Invalid sentinel values
  discovered include `0`, `5489`, and `32154` — all of which crash a direct cast to `DATE`.
  17 rows had invalid order dates.
- `sls_sales`, `sls_price`, and `sls_quantity` are interdependent under the business rule
  `Sales = Quantity × Price`. Multiple rows violated this — negative prices, zero or null
  sales, and inconsistent combinations.
- Referential integrity against Silver customer and product dimensions was validated before
  transformation and confirmed clean — no orphaned sales records.

**Transformation decisions and why:**
- **Integer date validation before casting:** A length check (`LENGTH(date::TEXT) <> 8`) and
  zero-value guard catch all known invalid sentinels before the cast. Invalid dates become
  `NULL` rather than crashing the load.
- **Financial field derivation order:** Price is resolved before sales, because the sales
  derivation depends on a clean price. Resolving both from the raw value simultaneously
  produces incorrect results when price itself is invalid.
- **`NULLIF(sls_quantity, 0)` as division guard:** When deriving price from
  `sales / quantity`, a zero quantity causes a division-by-zero error. `NULLIF` converts
  a zero denominator to `NULL`, making the result `NULL` instead of crashing the procedure.

---

#### `silver.erp_cust_az12` — ERP Customer Demographics

**Discoveries during exploration:**
- `cid` carries an inconsistent `NAS` prefix — present on some rows, absent on others.
  A direct join to `crm_cust_info.cst_key` fails for all prefixed rows. The split between
  prefixed and non-prefixed rows was confirmed by running a format audit.
- Some `bdate` values are set in the future, which is logically impossible.
- `gen` contains at least four representations of the same two values: `M`, `Male`, `F`,
  `Female`, plus NULLs and empty strings — discovered via a full distinct value audit with
  `UPPER(TRIM())` normalisation.

**Transformation decisions and why:**
- **Conditional prefix stripping (`CASE WHEN cid LIKE 'NAS%'`):** The prefix is not
  universal — unconditional stripping would corrupt rows that don't carry it. The condition
  was confirmed to be safe by the format audit.
- **Future birthdate nullification:** A customer cannot be born in the future. Setting these
  to `NULL` is safer than attempting correction — the error is preserved as an absence of
  data rather than a fabricated value.
- **Multi-variant gender conforming:** The `IN ('F', 'FEMALE')` pattern handles all known
  representations discovered during the distinct value audit in a single `CASE` branch.

---

#### `silver.erp_loc_a101` — Customer Location

**Discoveries during exploration:**
- `cid` contains `-` delimiters absent from `crm_cust_info.cst_key`. Discovered by placing
  both columns side by side — a purely structural formatting difference with no actual
  orphaned records once normalised.
- `cntry` contains NULL values, empty strings, and inconsistent country naming (`US`, `USA`,
  `United States` all representing the same country).

**Transformation decisions and why:**
- **`REPLACE(cid, '-', '')`:** The simplest fix for a purely structural mismatch. Confirmed
  safe by validating that after replacement, all `cid` values resolve in
  `silver.crm_cust_info.cst_key`.
- **`CASE WHEN` country standardisation with NULL/empty guard:** All known variants of each
  country are mapped to a single canonical label. NULLs and empty strings are explicitly
  caught and mapped to `'Unknown'`.

---

#### `silver.erp_px_cat_g1v2` — Product Category

**Discoveries during exploration:**
- All four columns passed every quality check: no whitespace, no unexpected values, no NULLs,
  no format inconsistencies.
- `cat` contains exactly four valid values: Bikes, Accessories, Clothing, Components.
- `maintenance` contains only `Yes` and `No`.
- One orphaned category record was found (a category with no matching products in
  `silver.crm_prd_info`) — noted but accepted as a valid edge case.

**Transformation decision and why:**
- **Pass-through load with no transformations:** Applying transformations where none are needed
  introduces unnecessary complexity and failure surface. The decision not to transform is
  itself a deliberate, documented engineering choice — not an oversight.

---

### Procedure Design Standards

Every Silver transformation is implemented as a **PostgreSQL stored procedure** following
a consistent set of standards across all six tables:

| Standard | Implementation | Why |
| :--- | :--- | :--- |
| **Idempotency** | `TRUNCATE` + `INSERT` in one transaction | Safe to re-run at any time without duplicating data |
| **Atomicity** | Single `BEGIN…END` plpgsql block | Either the full load commits or nothing does — no partial writes |
| **Error handling** | `EXCEPTION WHEN OTHERS THEN RAISE EXCEPTION` | Re-raises to trigger full rollback and signal failure to the caller |
| **Observability** | `RAISE NOTICE` on start and completion | Every run emits procedure name, rows inserted, rows rejected, and duration |
| **Data lineage** | `_source_system`, `_bronze_loaded_at`, `_silver_loaded_at` | Every Silver row is traceable to its origin system, raw landing time, and transformation run |
| **Data quality gate** | Pre-flight checks before `TRUNCATE` | Prevents destroying existing Silver data when the source is empty or critically malformed |
| **Least privilege** | `GRANT EXECUTE` to ETL role only (commented, ready to apply) | Procedures callable by the pipeline service account only |

### A Note on `RAISE EXCEPTION` in the Error Handler

Catching an error without re-raising it causes PostgreSQL to report the procedure as
**successful** — even if the `TRUNCATE` already ran and the `INSERT` never completed. The
result is an empty Silver table with a green pipeline status: the worst possible outcome.

`RAISE EXCEPTION` prevents this by escalating the failure to the full transaction level,
triggering a rollback of both the `TRUNCATE` and any partial `INSERT`, and returning a
clear failure signal to the caller. The Silver table is either fully reloaded or left
exactly as it was before the run started. This is the atomicity guarantee in practice.

---

## 🥇 Gold Layer — Star Schema

The Gold layer is the consumption-ready tier of the warehouse. It is modeled as a **Star Schema**
to provide a performant and intuitive experience for end-users and BI tools. All Gold objects
are implemented as **materialized views** over the Silver layer, physically storing the query
result for instant analyst access. They are refreshed each batch run via
`REFRESH MATERIALIZED VIEW CONCURRENTLY` — which rebuilds the data without locking the view,
so analysts continue querying uninterrupted during the refresh window.

### Model Overview
<img width="707" height="714" alt="image" src="https://github.com/user-attachments/assets/7485a023-a064-4334-9cf0-3e2eb780b9d1" />

```
gold.fact_sales
    ├── product_key  ──▶  gold.dim_products
    └── customer_key ──▶  gold.dim_customers
```

### `gold.dim_customers` — Customer Dimension

**Grain:** 1 row per unique customer.
**SCD Type:** Type 1 (overwrite). Always reflects the most current customer record.

Integrates `silver.crm_cust_info` (profiles), `silver.erp_cust_az12` (demographics), and
`silver.erp_loc_a101` (location). The cross-system join is made possible by the key
normalisation applied in the Silver layer.

| Column | Type | Description |
| :--- | :--- | :--- |
| `customer_key` | BIGINT | Generated surrogate key (PK). |
| `customer_id` | INTEGER | Natural key from source system (UK). |
| `customer_number` | TEXT | Business-facing customer reference. |
| `first_name` | TEXT | Customer first name. |
| `last_name` | TEXT | Customer last name. |
| `country` | TEXT | Normalised country name. |
| `marital_status` | TEXT | `{Married, Single, etc.}` |
| `gender` | TEXT | `{Male, Female, N/A}` |
| `birthdate` | DATE | Format: `YYYY-MM-DD`. |
| `created_date` | DATE | Initial registration date. |
| `_gold_processed_at` | TIMESTAMPTZ | Metadata: record update time. |
| `_source_system` | TEXT | Metadata: originating source. |

---

### `gold.dim_products` — Product Dimension

**Grain:** 1 row per unique, currently active product.

Integrates `silver.crm_prd_info` (product master) and `silver.erp_px_cat_g1v2` (category
hierarchy). The join is enabled by the composite key decomposition and delimiter normalisation
performed in the Silver layer. Only currently active products are surfaced (where `prd_end_dt IS NULL`).

| Column | Type | Description |
| :--- | :--- | :--- |
| `product_key` | BIGINT | Generated surrogate key (PK). |
| `product_id` | INTEGER | Source system identifier (UK). |
| `product_number` | TEXT | SKU / model number. |
| `product_name` | TEXT | Full product description. |
| `category_id` | TEXT | Product category code. |
| `category` | TEXT | High-level group (e.g., `Bikes`). |
| `subcategory` | TEXT | Detailed group (e.g., `Road Bikes`). |
| `maintenance` | BOOLEAN | Whether post-sale service is required. |
| `cost` | INTEGER | Base manufacturing/purchase cost. |
| `product_line` | TEXT | Brand/series classification. |
| `start_date` | DATE | Catalog entry date. |
| `_gold_processed_at` | TIMESTAMPTZ | Metadata: record update time. |
| `_source_system` | TEXT | Metadata: originating source. |

---

### `gold.fact_sales` — Sales Fact Table

**Grain:** 1 row per line item per order.

Sources from `silver.crm_sales_details`, joined to `gold.dim_customers` and `gold.dim_products`
via surrogate keys. The surrogate key join pattern means the fact table is insulated from
natural key format changes in the source systems.

| Column | Type        | Description |
| :--- |:------------| :--- |
| `order_number` | TEXT        | Transaction ID, e.g., `SO12345` (UK). |
| `product_key` | BIGINT      | FK → `gold.dim_products`. |
| `customer_key` | BIGINT      | FK → `gold.dim_customers`. |
| `order_date` | DATE        | Primary event date. |
| `shipping_date` | DATE        | Date of dispatch. |
| `due_date` | DATE        | Expected payment date. |
| `sales_amount` | INTEGER     | Net revenue for this line item. |
| `quantity` | INTEGER     | Unit count sold. |
| `price` | INTEGER     | Unit price at transaction time. |
| `_gold_processed_at` | TIMESTAMPTZ | Metadata: processing timestamp. |
| `_source_system` | TEXT        | Metadata: originating source. |

---

### Gold Layer Design Standards

| Standard | Implementation | Why |
| :--- | :--- | :--- |
| **Star Schema** | 1 fact table, 2 dimension tables | Optimised for analytical queries and BI tool compatibility |
| **Surrogate Keys** | `ROW_NUMBER()` generated `BIGINT` PKs | Decouples Gold from source system key formats and changes |
| **Materialized Views** | Physical storage of query results | Instant analyst access without re-querying Silver on every request |
| **Non-blocking Refresh** | `REFRESH MATERIALIZED VIEW CONCURRENTLY` | Analysts query uninterrupted during batch refresh window |
| **UTC Timestamps** | All timestamps use `TIMESTAMPTZ` | Consistent temporal tracking across time zones |
| **Active products only** | `WHERE prd_end_dt IS NULL` in `dim_products` | Only currently valid catalog entries are exposed to analysts |
| **Conformed dimensions** | Cross-system joins resolved in Silver | Gold views are simple and clean — complexity stays in Silver |

---

## 🧪 Test-Driven Development (TDD) for Data

This project follows a **Test-Driven Development** discipline applied to SQL. Before writing
any transformation procedure, a dedicated exploration script was written for each source
table to understand and document the data quality issues. After the Silver load, a separate
set of regression test scripts in `tests/silver/` validates the output.

### Test Coverage

Each test script covers:

| Test Category | What it checks |
| :--- | :--- |
| **Primary key validation** | No NULLs, no duplicates in the business key column |
| **Referential integrity** | Foreign key values in fact tables resolve to dimension records |
| **Whitespace checks** | No leading or trailing spaces in text columns |
| **Domain value audit** | Distinct values in low-cardinality columns match the expected set |
| **Business rule validation** | `Sales = Quantity × Price`, no future birthdates, valid date integer formats |
| **Transformation correctness** | Post-load checks confirming Silver output matches expected results |

Every test script ends with a **summary scoreboard** — a single query that aggregates all
individual checks into a pass/fail count. Running the test script after any Silver reload
gives an immediate, at-a-glance answer to: *is the data reliable?*

### Why This Approach Matters

The exploration phase for this project uncovered issues that are invisible to a simple row
count check:

- A composite key hiding a foreign key relationship inside a string (`crm_prd_info.prd_key`)
- A delimiter mismatch (`-` vs `_`) that causes silent join failures returning zero rows
- A date column storing integers with undocumented invalid sentinel values (`0`, `5489`, `32154`)
- A customer ID prefix (`NAS`) present on only some rows, making a direct join unreliable
- Business rule violations where `Sales ≠ Quantity × Price`
- A source system where `prd_end_dt < prd_start_dt` — invalid date ranges on product versions

None of these surface as errors. They manifest as wrong numbers in dashboards and missing
rows in reports. The test scripts make these checks repeatable and runnable after every
reload — so the Silver layer's reliability is not assumed, it is verified.

---

## 🛠️ Engineering Principles

| Principle | Implementation |
| :--- | :--- |
| **Medallion Architecture** | Three-schema separation: `bronze`, `silver`, `gold` |
| **ELT over ETL** | All transformation logic lives in SQL inside the database — no external tools |
| **Idempotency** | Every load procedure can be re-run safely at any time |
| **Atomicity** | `TRUNCATE` + `INSERT` in a single transaction — partial writes are impossible |
| **Test-Driven Development** | Exploration and regression test scripts written before and after each transformation |
| **Observability** | Structured `RAISE NOTICE` logs on every run with row counts and duration |
| **Data Lineage** | Every Silver row carries its source system, bronze load time, and silver load time |
| **Schema Isolation** | Each layer is independently queryable and auditable |
| **Zero-Footprint Data** | Datasets managed locally — only SQL logic and documentation are versioned |
| **Least Privilege** | ETL service role granted only `EXECUTE` on procedures, not direct table access |
| **Star Schema** | Gold layer modeled for analytical performance and BI tool compatibility |
| **Surrogate Keys** | Gold dimensions use generated keys, decoupling analytics from source system changes |
| **Scale-Aware Indexing** | Silver and Gold indexes designed around access patterns, not just current volume |
| **Non-blocking Refresh** | `REFRESH MATERIALIZED VIEW CONCURRENTLY` ensures zero analyst downtime during Gold rebuilds |

---

*Built with PostgreSQL · Medallion Architecture · Pure SQL Engineering*