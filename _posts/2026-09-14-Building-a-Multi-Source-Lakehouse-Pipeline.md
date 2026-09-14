---
layout: post
title: "Building a Multi-Source Lakehouse Pipeline with Python, REST APIs and Databricks"
date: 2026-09-14
categories: [Data Engineering, Databricks, AWS, Python, PostgreSQL]
tags: [Python, FastAPI, PostgreSQL, AWS S3, Databricks, PySpark, Delta Lake, Medallion Architecture, ETL, REST API]
---

# Building a Multi-Source Lakehouse Pipeline with Python, REST APIs and Databricks

## Introduction

This project demonstrates an end-to-end data engineering pipeline that brings customer, product and cart data into a Databricks lakehouse through a common Python REST API.

The underlying source systems are deliberately different:

- **Customers** are stored in PostgreSQL
- **Products** are stored in PostgreSQL
- **Carts** are stored as JSON data in an Amazon S3 bucket

A Python FastAPI application provides a consistent interface to all three datasets. Databricks consumes the API and processes the data through a medallion architecture consisting of Bronze, Silver and Gold layers.

The project demonstrates several common data engineering patterns:

- Relational database integration
- REST API development
- Object storage integration
- Semi-structured JSON processing
- PySpark transformations
- Delta Lake
- Incremental processing
- Data quality
- Workflow orchestration
- Source-system abstraction
- CI/CD and operational considerations

The high-level architecture is:

![Multi-source Databricks architecture](/assets/images/multi-source-databricks-architecture.png)

---

## 1. Solution Architecture

The pipeline contains three datasets:

| Dataset | Underlying source | API endpoint |
|---|---|---|
| Customers | PostgreSQL | `/customers` |
| Products | PostgreSQL | `/products` |
| Carts | Amazon S3 | `/carts` |

The important architectural point is that **Databricks does not need to know where the underlying data originates**.

The Python API provides a common ingestion interface:

```text
                    ┌─────────────────┐
                    │   PostgreSQL    │
                    │                 │
                    │ Customers       │
                    │ Products        │
                    └────────┬────────┘
                             │
                             │ SQL
                             │
                    ┌────────▼────────┐
                    │                 │
                    │   Python API    │
                    │    FastAPI      │
                    │                 │
                    └────────┬────────┘
                             │
                             │ REST / JSON
                             │
                    ┌────────▼────────┐
                    │   Databricks    │
                    │                 │
                    └────────┬────────┘
                             │
                             ▼
                       ┌──────────┐
                       │  Bronze  │
                       └────┬─────┘
                            │
                       ┌────▼─────┐
                       │  Silver  │
                       └────┬─────┘
                            │
                       ┌────▼─────┐
                       │   Gold   │
                       └──────────┘


                    ┌─────────────────┐
                    │    Amazon S3    │
                    │                 │
                    │ Carts JSON      │
                    └────────┬────────┘
                             │
                             │
                             ▼
                        Python API
```

The API effectively provides the following abstraction:

```text
API
│
├── /customers ──► PostgreSQL
│
├── /products  ──► PostgreSQL
│
└── /carts     ──► S3
```

This means that the downstream Databricks pipeline has a consistent interface regardless of the underlying source technology.

---

# 2. PostgreSQL Source

The PostgreSQL database contains the customer and product datasets.

A simplified customer table is:

```sql
CREATE TABLE customers (
    customer_id BIGINT PRIMARY KEY,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    email       VARCHAR(255),
    country     VARCHAR(100),
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP
);
```

The product table contains:

```sql
CREATE TABLE products (
    product_id   BIGINT PRIMARY KEY,
    product_name VARCHAR(255),
    category     VARCHAR(100),
    price        NUMERIC(12,2),
    active       BOOLEAN,
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP
);
```

The `updated_at` columns are particularly useful because they allow the API to support incremental extraction.

Rather than retrieving every record on every request, the API can return only records that have changed since a specified point in time.

---

# 3. Carts in Amazon S3

The carts dataset has a different underlying source.

Instead of being stored in PostgreSQL, cart data is provided as JSON files in an Amazon S3 bucket.

For example:

```text
s3://my-data-platform/carts/
    carts_2026-09-12.json
    carts_2026-09-13.json
    carts_2026-09-14.json
```

A cart record might look like:

```json
{
  "cart_id": 10001,
  "customer_id": 1001,
  "created_at": "2026-09-14T10:15:00",
  "items": [
    {
      "product_id": 501,
      "quantity": 2,
      "price": 19.99
    },
    {
      "product_id": 502,
      "quantity": 1,
      "price": 49.99
    }
  ]
}
```

This also demonstrates the ability to handle semi-structured data.

Importantly, Databricks does not read this S3 location directly. The Python API provides the interface to the carts data in the same way that it provides access to customers and products.

---

# 4. Building the Python REST API

The API is implemented using Python and FastAPI.

The application provides three endpoints:

```text
GET /customers
GET /products
GET /carts
```

The API acts as an abstraction layer between the source systems and the downstream data platform.

The underlying implementation of each endpoint can be different while presenting a consistent interface to API consumers.

---

# 5. Reading Customers from PostgreSQL

The customer repository function uses `psycopg` to query PostgreSQL:

```python
import psycopg


def get_customers():
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    customer_id,
                    first_name,
                    last_name,
                    email,
                    country,
                    created_at,
                    updated_at
                FROM customers
                ORDER BY customer_id
            """)

            return cur.fetchall()
```

The FastAPI endpoint converts the results into JSON:

```python
from fastapi import FastAPI

app = FastAPI()


@app.get("/customers")
def customers():

    rows = get_customers()

    return [
        {
            "customer_id": row[0],
            "first_name": row[1],
            "last_name": row[2],
            "email": row[3],
            "country": row[4],
            "created_at": row[5],
            "updated_at": row[6]
        }
        for row in rows
    ]
```

A response might look like:

```json
[
  {
    "customer_id": 1001,
    "first_name": "John",
    "last_name": "Smith",
    "email": "john.smith@example.com",
    "country": "UK",
    "created_at": "2026-01-10T09:15:00",
    "updated_at": "2026-09-10T14:32:00"
  }
]
```

---

# 6. Reading Products from PostgreSQL

Products follow the same pattern.

The database layer queries PostgreSQL:

```python
def get_products():

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    product_id,
                    product_name,
                    category,
                    price,
                    active,
                    created_at,
                    updated_at
                FROM products
                ORDER BY product_id
            """)

            return cur.fetchall()
```

The API exposes:

```text
GET /products
```

This gives consumers a consistent REST interface without exposing the PostgreSQL implementation.

---

# 7. Reading Carts from S3

The carts repository uses the AWS SDK for Python, `boto3`, to read the JSON data from S3.

A simplified implementation is:

```python
import boto3
import json

s3 = boto3.client("s3")


def get_carts():

    response = s3.get_object(
        Bucket="my-data-platform",
        Key="carts/carts.json"
    )

    return json.loads(
        response["Body"].read()
    )
```

The API endpoint is then straightforward:

```python
@app.get("/carts")
def carts():
    return get_carts()
```

The important point is that consumers of the API do not need to know that the carts originate in S3.

They simply request:

```text
GET /carts
```

This creates a clean separation between the API contract and the implementation of the underlying source.

---

# 9. Incremental API Extraction

The customer and product tables contain an `updated_at` column, which allows incremental extraction.

For example:

```text
GET /customers?updated_since=2026-09-13T00:00:00
```

The API can translate this into:

```sql
SELECT *
FROM customers
WHERE updated_at > %(updated_since)s
ORDER BY updated_at;
```

The same approach can be applied to products.

This means that after the initial load:

```text
Initial execution
       │
       └── Extract all records
```

subsequent executions can extract only changes:

```text
Subsequent execution
       │
       └── Extract changed records
```

For large datasets, pagination should also be used so that a single API response does not become excessively large.

---

# 10. Carts and Incremental Processing

The appropriate incremental strategy for carts depends on how the S3 data is produced.

If each file represents a new immutable batch, for example:

```text
carts_2026-09-12.json
carts_2026-09-13.json
carts_2026-09-14.json
```

then the API can expose newly arrived data without repeatedly processing historical files.

If carts can be modified after their initial creation, then the API needs to expose sufficient information to identify changed records.

Possible approaches include:

- `updated_at`
- Batch timestamps
- File timestamps
- Record version numbers
- Source-system change indicators

The downstream Databricks pipeline can then use the appropriate merge or append strategy.

---

# 11. Databricks API Ingestion

Databricks consumes all three datasets through the REST API.

For example:

```python
import requests

response = requests.get(
    f"{API_URL}/customers",
    timeout=60
)

response.raise_for_status()

customers = response.json()

customers_df = spark.createDataFrame(customers)
```

Products are retrieved in the same way:

```python
response = requests.get(
    f"{API_URL}/products",
    timeout=60
)

response.raise_for_status()

products = response.json()

products_df = spark.createDataFrame(products)
```

And carts:

```python
response = requests.get(
    f"{API_URL}/carts",
    timeout=60
)

response.raise_for_status()

carts = response.json()

carts_df = spark.createDataFrame(carts)
```

The source-specific complexity therefore remains behind the API.

---

# 12. Bronze Layer

The Bronze layer provides several benefits.

It allows the original ingested data to be retained independently of downstream transformations.

If a problem is subsequently discovered in Silver or Gold, the Bronze data can be inspected to determine whether the problem originated in:

- The source
- The API
- The ingestion process
- A transformation

This also provides a useful recovery point for downstream processing.

The raw API data is first written to the Bronze layer.

The resulting Delta tables are:

```text
bronze.customers
bronze.products
bronze.carts
```

Operational metadata can be added during ingestion:

```python
from pyspark.sql.functions import current_timestamp, lit

customers_df = (
    customers_df
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_system", lit("postgres-api"))
)
```

The same principle can be applied to products and carts.

For example, carts can be identified as coming from the S3-backed API:

```python
carts_df = (
    carts_df
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_system", lit("s3-api"))
)
```

This provides useful lineage information.

The Bronze layer therefore provides a durable representation of what was received from the API.

---

# 14. Silver Layer

The Silver layer contains cleaned, validated and standardised datasets.

For customers:

```python
from pyspark.sql.functions import col, trim, lower

customers_silver = (
    customers_df
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
)
```

Duplicate customers can be removed using the business key:

```python
customers_silver = customers_silver.dropDuplicates(
    ["customer_id"]
)
```

Products can undergo similar transformations.

The resulting tables are:

```text
silver.customers
silver.products
```

---

# 15. Transforming the Carts Dataset

The carts data contains a nested `items` array.

A typical Spark schema is:

```text
root
 |-- cart_id: long
 |-- customer_id: long
 |-- created_at: timestamp
 |-- items: array
 |    |-- element: struct
 |    |    |-- product_id: long
 |    |    |-- quantity: integer
 |    |    |-- price: double
```

For analytical purposes, the nested items can be flattened.

First, the array is exploded:

```python
from pyspark.sql.functions import explode

cart_items = (
    carts_df
    .select(
        "cart_id",
        "customer_id",
        "created_at",
        explode("items").alias("item")
    )
)
```

The nested fields can then be projected into columns:

```python
cart_items = cart_items.select(
    "cart_id",
    "customer_id",
    "created_at",
    col("item.product_id").alias("product_id"),
    col("item.quantity").alias("quantity"),
    col("item.price").alias("price")
)
```

The result is a much more useful analytical structure:

```text
cart_id
customer_id
created_at
product_id
quantity
price
```

This can be stored as:

```text
silver.cart_items
```

---

# 16. Data Quality

Data quality checks are applied during Silver processing.

For customers:

```text
customer_id must not be NULL
email must be valid
```

For products:

```text
product_id must not be NULL
price must be >= 0
```

For carts:

```text
cart_id must not be NULL
customer_id must not be NULL
product_id must not be NULL
quantity must be > 0
price must be >= 0
```

Invalid records can be separated from valid records and written to quarantine tables:

```text
quarantine.customers
quarantine.products
quarantine.carts
```

This allows bad data to be investigated without contaminating the main analytical datasets.

---

# 17. Gold Layer

The Gold layer contains business-oriented datasets.

This is where data from the different source systems can be combined to answer business questions.

For example, customer and cart information can be combined:

```sql
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT ci.cart_id) AS cart_count,
    SUM(ci.quantity * ci.price) AS cart_value
FROM silver.customers c
LEFT JOIN silver.cart_items ci
    ON c.customer_id = ci.customer_id
GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name;
```

This could produce:

```text
gold.customer_cart_summary
```

Similarly, cart items can be joined with products:

```sql
SELECT
    ci.product_id,
    p.product_name,
    p.category,
    SUM(ci.quantity) AS quantity_added,
    SUM(ci.quantity * ci.price) AS cart_value
FROM silver.cart_items ci
JOIN silver.products p
    ON ci.product_id = p.product_id
GROUP BY
    ci.product_id,
    p.product_name,
    p.category;
```

This creates:

```text
gold.product_cart_summary
```

The Gold layer therefore represents business concepts rather than simply reproducing the source-system structures.

---

# 18. Databricks Workflow

The pipeline can be orchestrated using a Databricks Workflow.

A possible workflow is:

```text
                 ┌─────────────────────┐
                 │ Extract Customers   │
                 │      from API       │
                 └──────────┬──────────┘
                            │
                 ┌──────────▼──────────┐
                 │ Extract Products    │
                 │      from API       │
                 └──────────┬──────────┘
                            │
                 ┌──────────▼──────────┐
                 │ Extract Carts       │
                 │      from API       │
                 └──────────┬──────────┘
                            │
                       ┌────▼────┐
                       │ Bronze  │
                       └────┬────┘
                            │
                       ┌────▼────┐
                       │ Silver  │
                       └────┬────┘
                            │
                       ┌────▼────┐
                       │  Gold   │
                       └─────────┘
```

The three API extraction tasks can potentially run independently or in parallel.

The downstream Silver processing is dependent on successful Bronze ingestion.

---

# 19. Delta Lake and Upserts

The Databricks tables use Delta Lake.

Delta provides ACID transactions and supports upserts using `MERGE`.

For example:

```sql
MERGE INTO silver.customers AS target

USING bronze.customers AS source

ON target.customer_id = source.customer_id

WHEN MATCHED THEN
    UPDATE SET *

WHEN NOT MATCHED THEN
    INSERT *;
```

This allows changed customer records to be incorporated without rebuilding the complete Silver dataset.

Products can use the same approach.

For carts, the correct strategy depends on the source semantics.

If cart records are immutable, append processing may be sufficient.

If existing carts can change, a merge based on the cart business key may be more appropriate.

---

# 21. Security

Database credentials should never be embedded directly in application or notebook code.

They should be stored using an appropriate secrets-management mechanism.

The API's access to S3 should similarly use an AWS identity with only the permissions required to read the relevant data.

The architecture therefore follows the principle of least privilege.

Conceptually:

```text
Databricks
    │
    │ API credentials
    ▼
Python API
    │
    ├── Database credentials
    │        │
    │        ▼
    │    PostgreSQL
    │
    └── AWS credentials / role
             │
             ▼
             S3
```

---

# 22. CI/CD

The Python API and Databricks code are maintained in Git and deployed through CI/CD via Render.

---

# 25. Why This Architecture?

The main architectural decision in this project is the separation between **source systems, the API and the analytical platform**.

The underlying sources are different:

```text
PostgreSQL
     │
     ├── Customers
     └── Products

S3
     │
     └── Carts
```

But the downstream interface is consistent:

```text
          Python API
              │
     ┌────────┼────────┐
     │        │        │
 Customers Products  Carts
     │        │        │
     └────────┼────────┘
              │
              ▼
         Databricks
```

This means that Databricks is decoupled from the implementation details of the source systems.

The API provides a clear contract between the source layer and the data platform.

The medallion architecture provides a second level of separation:

```text
Bronze
  ↓
Raw ingested data

Silver
  ↓
Cleaned and standardised data

Gold
  ↓
Business-ready data
```

This makes the overall platform easier to evolve and maintain.

---

# Conclusion

This project demonstrates a multi-source data engineering architecture in which PostgreSQL and S3 data are exposed through a common Python REST API and consumed by Databricks.

The complete flow is:

```text
PostgreSQL ──┐
             │
             ▼
          Python API
             │
S3 ──────────┤
             │
             ▼
          Databricks
             │
             ▼
           Bronze
             │
             ▼
           Silver
             │
             ▼
            Gold
```

The project combines traditional relational data with semi-structured object-storage data while keeping the downstream ingestion interface consistent.

More importantly, it demonstrates several principles that are applicable to production data platforms: separation of concerns, source abstraction, incremental processing, data quality, reliable orchestration, security, observability and CI/CD.

The architecture could be extended with additional datasets and source systems without requiring the core Databricks transformation architecture to change.

---

## Project Source Code

The complete implementation, including the Python API and Databricks processing code, is available in the accompanying GitHub repository.

**GitHub:** [Add repository link here]