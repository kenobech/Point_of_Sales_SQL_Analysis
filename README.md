# Point_of_Sales_SQL_Analysis
I designed and implemented a full end-to-end Point of Sale ETL and Analytics Pipeline for an 800k-row dataset, including database creation, schema design, bulk ingestion, profiling, normalization, error handling, automation via SQL Agent, and analytical reporting. The solution uses MERGE-based upserts, structured schemas, automated logging/auditing, daily ETL scheduling, indexed analytical tables, and KPI computation for strategic insights.

---

# **Introduction**

The Point of Sale (PoS) dataset for **Hugging Face Ltd.** contains **800,000 rows** and **11 columns**, namely:
- transaction_id,
- store_name,
- city,
- transaction_date,
- cashier_id,
- items_count,
- total_amount_ngn,
- payment_method,
- discount_applied,
- loyalty_points_earned, and
- receipt_number.

Below is the structured workflow I followed in designing and implementing the complete PoS data engineering and analytics pipeline.

---

## **1. Database Setup**

I began by creating a new SQL Server database called **RetailPoSDB**. This database acts as the central repository for all stages of the ETL workflow—from raw ingestion to analytical reporting.

---

## **2. Schema Design**

To maintain a clean, scalable, and highly organized warehouse structure, I created the following schemas:

### **• staging schema**

Used for raw data ingestion via **BULK INSERT**. No transformations occur here; it's a landing zone for the CSV file.

### **• ref schema**

Holds reference (dimension) tables such as

* **ref.stores** (store_name, city)
* **ref.payment_methods** (payment_methods, payment_id)

These serve as master data tables to enforce consistency and support referential integrity.

### **• sales schema**

Contains the cleaned and normalized **fact table**:

* **sales.transactions**, which stores transaction-level facts mapped to store_id and payment_id from reference tables.

### **• analytics schema**

Used for aggregated insights and reporting outputs, e.g.:

* **analytics.daily_summary**
* Views such as **vw_daily_revenue**, **vw_revenue_per_store**

### **• log schema**

Includes structured logging tables for:

* ETL errors
* Audit trails
* Metadata for pipeline monitoring
  
---

## **3. Staging Layer & Raw Data Ingestion**

Next, I created **staging.PoSRaw**, a denormalized table that mirrors the CSV structure.

Using **BULK INSERT**, I loaded all 800k records directly into the staging table. This step ensures optimal loading performance before any cleaning or transformations.

---

## **4. Data Profiling & Anomaly Detection**

Before normalization, I performed detailed profiling to understand data quality and identify anomalies:

### ** Duplicate checks**

* Duplicate *receipt_number*
* Duplicate *store_name*
* Duplicate *city*
* Duplicate *transaction_date*

### ** Null analysis**

Identified which columns had nulls and to what extent.

### **Outlier detection**

Flagged abnormal large values of `items_count` and `total_amount_ngn`.

### ** Invalid dates**

Checked for future timestamps and transactions recorded outside business hours (8 AM – 10 PM).

This profiling step was crucial for determining the cleaning intensity needed and identifying normalization opportunities.

---

## **5. Normalization & Creation of Dimension and Fact Tables**

From the profiling results, I determined that several fields, such as **store_name**, **city**, and **payment_method**, were repeated hundreds or thousands of times.

To eliminate redundancy and ensure data integrity normalization to 3NF was necessary. the raw table was denormalised, I had to normalise it innto the following tables:

### **Created dimension tables:**

* **ref.stores** (distinct store_name + city combinations)
* **ref.payment_methods** (distinct payment methods)

### **Created fact table:**

* **sales.transactions**

The fact table captures all transactional metrics—items_count, total_amount_ngn, loyalty points, etc.—but references store and payment attributes through **foreign keys** rather than repeating text values.

---

## **6. ETL Pipeline & Master MERGE Procedure**

I developed a robust ETL pipeline implemented through:

### **etl.sp_master_etl_merge**

A single stored procedure that handles:

* Upsert (MERGE) logic for stores
* Upsert (MERGE) logic for payment methods
* Deduplication using ROW_NUMBER
* Insert/update of the sales transactions fact table
* Explicit TRY…CATCH error handling
* Automatic error logging into **log.error_log**

The ETL is fully incremental and idempotent, meaning it can run repeatedly without duplicating data.

---

## **7. SQL Server Agent Job (ETL Automation)**

To automate the pipeline, I created a scheduled SQL Agent Job called:

### **PoS_ETL_Daily_Refresh**

* Runs **daily at 1:00 AM**
* Executes the master ETL procedure
* Includes automatic retries
* Logged execution status and row counts

This converts the entire system into a production-ready, self-refreshing data pipeline.

---

## **8. Analytical Layer & KPIs**

Using normalized and cleaned data, I implemented analytic queries that cover:

### **Insights**
From the analysis, I found that there are 10 stores, 15 cities, 900 cashiers, and 800,000 receipts, which corresponds to 800,000 transactions. The best-performing cashiers by sales volume are CASH763 and CASH460, both recording product sales worth over 100 million. The least-performing are CASH410, CASH943, CASH738, and CASH106, each with sales of approximately 70 million.

The total revenue generated is ₦80 billion, with an average transaction value of ₦100,435.005367. There are 160,246 discounted transactions and 639,754 non-discounted transactions. Total accumulated loyalty points stand at 400,438,676.

Among the cities, Abeoluta is the highest-grossing in terms of revenue, while Pointek is the top-performing store. The highest revenue date recorded is 16-09-2024.

Regarding payment methods, cash leads with 400,000 transactions, followed by card with 319,000, and mobile money with 79,000. A similar trend is observed in revenue:

Cash: ~₦40 billion

Card: ~₦32 billion

Mobile Money: ~₦8 billion

This clearly shows that cash is the most preferred mode of payment. Cash transactions also have the highest number of discounted transactions as well as the highest number of full-price transactions, while mobile money consistently records the lowest counts in both categories. Even when broken down by store, cash remains the dominant payment method.

To analyze cashier performance, I used a variable and a CTE. For example, in the month of January, 465 cashiers met the set target of ₦9 million, while 435 did not. This is an important function because it can be used to determine bonus allocations, making it easy to identify which cashiers deserve incentives based on performance in a specific month.

Using the same logic, I also evaluated store performance. With a target of ₦9 million, Balogun Market met its target for January, indicating that it is operating within the required threshold.

In terms of weekly revenue trends, Week 13 generates the highest revenue, while Week 44 generates the lowest. August is the highest-grossing month, and October is the lowest. Overall, the 4th quarter is the highest-performing, and the 1st quarter is the lowest.

------

