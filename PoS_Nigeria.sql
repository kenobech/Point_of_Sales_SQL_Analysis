-- ===========================================================================================================================================
-- POINT OF SALES DATABASE PROJECT
-- FULL WORKFLOW: Database Setup | ETL Pipeline | Error Handling | Profiling | Analytics | 
-- ===========================================================================================================================================

-- ===========================================================================================================================================
-- 1. DATABASE CREATION
-- ===========================================================================================================================================
IF DB_ID (N'RetailPoSDB') IS NULL
BEGIN
CREATE DATABASE RetailPoSDB;
END;
GO

USE RetailPoSDB;
GO
-- ===========================================================================================================================================
-- 2. SCHEMA CREATION
-- ===========================================================================================================================================
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'sales')
    EXEC('CREATE SCHEMA sales');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'analytics')
    EXEC('CREATE SCHEMA analytics');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ref')
    EXEC('CREATE SCHEMA ref');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'log')
    EXEC('CREATE SCHEMA log');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
    EXEC('CREATE SCHEMA etl');
GO
-- ===========================================================================================================================================
--3. ERROR LOGGING/AUDIT
-- ===========================================================================================================================================
--3.1 Error logging table
IF OBJECT_ID ('log.error_log','U') IS NOT NULL DROP TABLE log.error_log;
GO
CREATE TABLE log.error_log (
    error_id INT IDENTITY(1,1) PRIMARY KEY,
    error_time DATETIME DEFAULT GETDATE(),
    error_message NVARCHAR(MAX),
    error_number INT,
    error_severity INT,
    error_state INT,
    error_line INT,
    procedure_name NVARCHAR(200)
);
GO

--3.2 Audit Table
IF OBJECT_ID('log.etl_audit','U') IS NOT NULL DROP TABLE log.etl_audit;
GO
CREATE TABLE log.etl_audit (
    audit_id INT IDENTITY(1,1) PRIMARY KEY,
    run_time DATETIME2(3) DEFAULT SYSDATETIME(),
    status NVARCHAR(20),
    rows_processed INT NULL,
    message NVARCHAR(4000) NULL
);
GO

--3.3 Error logging procedure
CREATE PROCEDURE log.sp_log_error
AS
BEGIN
    INSERT INTO log.error_log (
        error_message,
        error_number,
        error_severity,
        error_state,
        error_line,
        procedure_name
    )
    SELECT 
        ERROR_MESSAGE(),
        ERROR_NUMBER(),
        ERROR_SEVERITY(),
        ERROR_STATE(),
        ERROR_LINE(),
        ERROR_PROCEDURE();
END;
GO

-- ===========================================================================================================================================
-- 4. STAGING TABLE FOR RAW DATA-RAW CSV INPUT TABLE
-- ===========================================================================================================================================
IF OBJECT_ID ('staging.PoSRaw','U') IS NOT NULL DROP TABLE staging.PoSRaw;
GO
CREATE TABLE staging.PoSRaw (
    transaction_id NVARCHAR (100),
    store_name NVARCHAR(100),
    city NVARCHAR(100),
    transaction_date DATETIME,
    cashier_id NVARCHAR(50),
    items_count INT,
    total_amount_ngn DECIMAL(12,2),
    payment_method NVARCHAR(100),
    discount_applied NVARCHAR(100),
    loyalty_points_earned INT,
    receipt_number NVARCHAR(100)
);
GO
-- ===========================================================================================================================================
-- 5. BULK IMPORT RAW DATA
-- ===========================================================================================================================================
BULK INSERT staging.PoSRaw
FROM 'C:\Users\LENOVO\Downloads\nigerian_retail_and_ecommerce_point_of_sale_records.csv'
WITH (
    FORMAT = 'CSV',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    FIRSTROW = 2,
    TABLOCK,
    KEEPNULLS,
    CODEPAGE = '65001'
);
GO

-- ===========================================================================================================================================
-- 6. DATA PROFILING & ANOMALY DETECTION
-- ===========================================================================================================================================
-- 6.1. View Raw Data
SELECT * FROM staging.PoSRaw;
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES;
SELECT * FROM INFORMATION_SCHEMA.TABLES;

--6.2. Detect Duplicate Transactions
SELECT 
    receipt_number,
    COUNT(*) AS duplicate_count
FROM staging.PoSRaw
GROUP BY receipt_number
HAVING COUNT(*) > 1;

--6.3. Duplicate Stores
SELECT 
	store_name,
	COUNT(*) AS duplicate_stores
FROM staging.PoSRaw
GROUP BY store_name
HAVING COUNT(*)>1;

--6.4 Duplicate City
SELECT
	city,
	COUNT(*) AS duplicate_city
FROM staging.PoSRaw
GROUP BY city
HAVING COUNT(*)>1;
select * from staging.PoSRaw

--6.5 Null Detection
SELECT 
    SUM(CASE WHEN transaction_id IS NULL THEN 1 END) AS null_transaction_id,
    SUM(CASE WHEN store_name IS NULL THEN 1 END) AS null_store_name,
    SUM(CASE WHEN city IS NULL THEN 1 END) AS null_city,
    SUM(CASE WHEN transaction_date IS NULL THEN 1 END) AS null_txn_date,
    SUM(CASE WHEN cashier_id IS NULL THEN 1 END) AS null_cashier,
    SUM(CASE WHEN items_count IS NULL THEN 1 END) AS null_items,
    SUM(CASE WHEN total_amount_ngn IS NULL THEN 1 END) AS null_amount
FROM staging.PoSRaw;

--6.6 Outlier Detection
SELECT *
FROM staging.PoSRaw
WHERE total_amount_ngn > 200000 OR items_count > 200;

--6.7 Invalid Dates
SELECT *
FROM staging.PoSRaw
WHERE transaction_date > GETDATE();

-- 6.8 Duplicate Transaction Date
SELECT 
    transaction_date,
    COUNT(*) AS duplicate_count
FROM staging.PoSRaw
GROUP BY 
    transaction_date
HAVING COUNT(*) > 1;

--6.9 Transactions Outside of Business Hours
SELECT 
    transaction_id,
    store_name,
    cashier_id,
    transaction_date,
    DATEPART(HOUR, transaction_date) AS transaction_hour
FROM staging.PoSRaw
WHERE DATEPART(HOUR, transaction_date) NOT BETWEEN 8 AND 22;

-- ===========================================================================================================================================
-- 7. NORMALIZED TABLES
-- ===========================================================================================================================================
-- 9.1 Create Reference Data Tables - stores & payment_methods
IF OBJECT_ID ('ref.stores','U') IS NOT NULL DROP TABLE ref.stores;
GO
CREATE TABLE ref.stores (
    store_id INT IDENTITY(1,1) PRIMARY KEY,
    store_name NVARCHAR(100),
    city NVARCHAR(100)
);
GO

IF OBJECT_ID ('ref.payment_methods','U') IS NOT NULL DROP TABLE ref.payment_methods;
GO
CREATE TABLE ref.payment_methods (
    payment_id INT IDENTITY(1,1) PRIMARY KEY,
    payment_method NVARCHAR(100)
);
GO
--8.2 Create Transactions Table
IF OBJECT_ID ('sales.transactions','U') IS NOT NULL DROP TABLE sales.transactions;
GO
CREATE TABLE sales.transactions (
    transaction_id NVARCHAR (100) PRIMARY KEY,
    store_id INT,
    cashier_id NVARCHAR(50),
    transaction_date DATETIME,
    items_count INT,
    total_amount_ngn DECIMAL(12,2),
    payment_id INT,
    discount_applied NVARCHAR(100),
    loyalty_points_earned INT,
    receipt_number NVARCHAR(100),
    FOREIGN KEY (store_id) REFERENCES ref.stores(store_id),
    FOREIGN KEY (payment_id) REFERENCES ref.payment_methods(payment_id)
);
GO
--8.3 Create analytics Table
IF OBJECT_ID ('analytics.daily_summary', 'U') IS NOT NULL DROP TABLE analytics.daily_summary;
GO
CREATE TABLE analytics.daily_summary (
    summary_id INT IDENTITY(1,1) PRIMARY KEY,
    report_date DATE,
    total_transactions INT,
    total_revenue DECIMAL(18,2),
    transactions_with_discount INT,
	transactions_without_discount INT,
    total_loyalty_points INT,
    top_city NVARCHAR(100),
    top_store NVARCHAR(100)
);
GO
-- ===========================================================================================================================================
-- 9. DATA INSERTION
-- ===========================================================================================================================================
--9.1 Populate ref.stores table
INSERT INTO ref.stores (store_name, city)
SELECT DISTINCT store_name, city
FROM staging.PoSRaw
WHERE store_name IS NOT NULL
  AND city IS NOT NULL;

--9.2 Populate ref.payment_methods
INSERT INTO ref.payment_methods (payment_method)
SELECT DISTINCT payment_method
FROM staging.PoSRaw
WHERE payment_method IS NOT NULL;

--9.3 Populate sales.transactions
INSERT INTO sales.transactions (
    transaction_id,
    store_id,
    cashier_id,
    transaction_date,
    items_count,
    total_amount_ngn,
    payment_id,
    discount_applied,
    loyalty_points_earned,
    receipt_number
)
SELECT
    s.transaction_id,      
    st.store_id,
    s.cashier_id,
    s.transaction_date,
    s.items_count,
    s.total_amount_ngn,
    pm.payment_id,
    s.discount_applied,     
    s.loyalty_points_earned,
    s.receipt_number
FROM staging.PoSRaw s
INNER JOIN ref.stores st
    ON s.store_name = st.store_name
   AND s.city = st.city
INNER JOIN ref.payment_methods pm
    ON s.payment_method = pm.payment_method;

-- ===========================================================================================================================================
--10. MASTER ETL STORED PROCEDURE
-- ===========================================================================================================================================
IF OBJECT_ID(N'etl.sp_master_etl_merge','P') IS NOT NULL
    DROP PROCEDURE etl.sp_master_etl_merge;
GO

CREATE PROCEDURE etl.sp_master_etl_merge
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRAN;

-------------------------------------------------------------
        -- 1) UPSERT STORES
-------------------------------------------------------------
MERGE ref.stores AS tgt
        USING (
            SELECT DISTINCT store_name, city
            FROM staging.PoSRaw
        ) AS src
        ON tgt.store_name = src.store_name
           AND ISNULL(tgt.city,'') = ISNULL(src.city,'')
        WHEN MATCHED THEN
            UPDATE SET city = src.city
        WHEN NOT MATCHED THEN
            INSERT (store_name, city)
            VALUES (src.store_name, src.city);
-------------------------------------------------------------
-- 2) UPSERT PAYMENT METHODS
-------------------------------------------------------------
MERGE ref.payment_methods AS tgt2
        USING (
            SELECT DISTINCT payment_method
            FROM staging.PoSRaw
        ) AS src2
        ON tgt2.payment_method = src2.payment_method
        WHEN NOT MATCHED THEN
            INSERT (payment_method)
            VALUES (src2.payment_method);
-------------------------------------------------------------
-- 3) MERGE INTO FACT TABLE: sales.transactions
-------------------------------------------------------------
MERGE sales.transactions AS tgt3
        USING (
            SELECT 
                d.transaction_id,
                d.store_name,
                d.city,
                d.transaction_date,
                d.cashier_id,
                d.items_count,
                d.total_amount_ngn,
                d.payment_method,
                d.discount_applied,
                d.loyalty_points_earned,
                d.receipt_number
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY transaction_date) AS rn
                FROM staging.PoSRaw
            ) d
            WHERE d.rn = 1
        ) AS src3
        ON tgt3.transaction_id = src3.transaction_id

        WHEN MATCHED THEN
            UPDATE SET
                store_id = (
                    SELECT store_id
                    FROM ref.stores
                    WHERE store_name = src3.store_name
                      AND ISNULL(city,'') = ISNULL(src3.city,'')
                ),
                transaction_date = TRY_CAST(src3.transaction_date AS DATETIME2(3)),
                cashier_id = src3.cashier_id,
                items_count = TRY_CAST(src3.items_count AS INT),
                total_amount_ngn = TRY_CAST(src3.total_amount_ngn AS DECIMAL(18,2)),
                payment_id = (
                    SELECT payment_id
                    FROM ref.payment_methods
                    WHERE payment_method = src3.payment_method
                ),
                discount_applied = src3.discount_applied,
                loyalty_points_earned = TRY_CAST(src3.loyalty_points_earned AS INT),
                receipt_number = src3.receipt_number

        WHEN NOT MATCHED THEN
            INSERT (
                transaction_id,
                store_id,
                cashier_id,
                transaction_date,
                items_count,
                total_amount_ngn,
                payment_id,
                discount_applied,
                loyalty_points_earned,
                receipt_number
            )
            VALUES (
                src3.transaction_id,
                (SELECT store_id
                 FROM ref.stores
                 WHERE store_name = src3.store_name
                   AND ISNULL(city,'') = ISNULL(src3.city,'')),
                src3.cashier_id,
                TRY_CAST(src3.transaction_date AS DATETIME2(3)),
                TRY_CAST(src3.items_count AS INT),
                TRY_CAST(src3.total_amount_ngn AS DECIMAL(18,2)),
                (SELECT payment_id
                 FROM ref.payment_methods
                 WHERE payment_method = src3.payment_method),
                src3.discount_applied,
                TRY_CAST(src3.loyalty_points_earned AS INT),
                src3.receipt_number
            );
-------------------------------------------------------------
-- COMMIT TRANSACTION
-------------------------------------------------------------
COMMIT TRAN;
    END TRY

    BEGIN CATCH
        ROLLBACK TRAN;

        INSERT INTO log.error_log (
            error_message,
            error_number,
            error_severity,
            error_state,
            error_line,
            procedure_name
        )
        SELECT
            LEFT(ISNULL(ERROR_MESSAGE(),'NULL'),4000),
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_LINE(),
            'etl.sp_master_etl_merge';

        EXEC log.sp_log_error;

        THROW;
    END CATCH
END;
GO
-- ===========================================================================================================================================
-- 11. INDEXES FOR PERFORMANCE
-- ===========================================================================================================================================
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_transactions_transaction_date' AND object_id=OBJECT_ID('sales.transactions'))
    CREATE INDEX IX_transactions_transaction_date ON sales.transactions(transaction_date);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_transactions_store_id' AND object_id=OBJECT_ID('sales.transactions'))
    CREATE INDEX IX_transactions_store_id ON sales.transactions(store_id);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_transactions_payment_id' AND object_id=OBJECT_ID('sales.transactions'))
    CREATE INDEX IX_transactions_payment_id ON sales.transactions(payment_id);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_stores_name_city' AND object_id=OBJECT_ID('ref.stores'))
    CREATE INDEX IX_stores_name_city ON ref.stores(store_name, city);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_payment_methods_method' AND object_id=OBJECT_ID('ref.payment_methods'))
    CREATE INDEX IX_payment_methods_method ON ref.payment_methods(payment_method);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_cashier' AND object_id=OBJECT_ID('sales.transactions'))
    CREATE INDEX IX_cashier ON sales.transactions(cashier_id);
GO
-- ===========================================================================================================================================
-- 12. ANALYSIS
-- ===========================================================================================================================================
--12.1 Store Count
SELECT 
	COUNT(DISTINCT store_name) AS Store_Count
FROM ref.stores;

--12.2 City Count
SELECT 
	COUNT(DISTINCT city) AS City_count
FROM ref.stores;

--12.3 Cashier Count
SELECT 
	COUNT(DISTINCT cashier_id) AS Cashier_Count
FROM sales.transactions;

--12.4 Receipt count
SELECT 
	COUNT(DISTINCT receipt_number) AS ReceiptCount
FROM sales.transactions;

-- 12.5 Total Item count
SELECT SUM(items_count) AS Total_Items_Count
FROM sales.transactions;

--12.6 Best Selling Cashier
SELECT DISTINCT cashier_id, SUM(items_count) AS TotalItems, SUM(total_amount_ngn) AS TotalSales
FROM sales.transactions
GROUP BY cashier_id
ORDER BY TotalSales DESC;

--12.7 Total Revenue Overview
SELECT 
    COUNT(*) AS total_transactions,
    SUM(total_amount_ngn) AS total_revenue,
	SUM(total_amount_ngn) / COUNT(*) AS avg_transaction_value,
    SUM(CASE WHEN discount_applied IN ('TRUE','True','1') THEN 1 ELSE 0 END) AS transactions_with_discount,
    SUM(CASE WHEN discount_applied IN ('FALSE','False','0') THEN 1 ELSE 0 END) AS transactions_without_discount,
    SUM(loyalty_points_earned) AS total_loyalty_points
FROM sales.transactions;
--12.8 Revenue by City 
SELECT 
    st.city,
    SUM(total_amount_ngn) AS total_revenue
FROM sales.transactions t
JOIN ref.stores st ON t.store_id = st.store_id
GROUP BY st.city
ORDER BY total_revenue DESC;

--12.9 Top 5 Stores in revenue by City 
WITH CityRevenue AS (
    SELECT 
        st.city,
        SUM(t.total_amount_ngn) AS total_revenue,
        RANK() OVER (PARTITION BY st.city ORDER BY SUM(t.total_amount_ngn) DESC) AS revenue_rank
    FROM sales.transactions t
    JOIN ref.stores st ON t.store_id = st.store_id
    GROUP BY st.city
)
SELECT city, total_revenue, revenue_rank
FROM CityRevenue
WHERE revenue_rank <= 5
ORDER BY city, revenue_rank;

--12.10 Store Revenue Contribution %
SELECT
    st.store_name,
    SUM(s.total_amount_ngn) AS total_revenue,
    ROUND(
        100.0 * SUM(s.total_amount_ngn) / SUM(SUM(s.total_amount_ngn)) OVER (),
        2
    ) AS revenue_percentage
FROM sales.transactions s
JOIN ref.stores st ON s.store_id = st.store_id
GROUP BY st.store_name
ORDER BY total_revenue DESC;

--12.11 Top Performing Cashiers by Revenue
SELECT 
    cashier_id,
    COUNT(*) AS transactions_count,
    SUM(total_amount_ngn) AS total_revenue,
    SUM(total_amount_ngn) / COUNT(*) AS avg_ticket_value,
    RANK() OVER (ORDER BY SUM(total_amount_ngn) DESC) AS sales_rank
FROM sales.transactions
GROUP BY cashier_id
ORDER BY sales_rank;

--12.12 Top Revenue Days
SELECT TOP 10
    CAST(transaction_date AS DATE) AS report_date,
    SUM(total_amount_ngn) AS daily_revenue
FROM sales.transactions
GROUP BY CAST(transaction_date AS DATE)
ORDER BY daily_revenue DESC;
-- ===========================================================================================================================================
-- 13. Payment method analysis
-- ===========================================================================================================================================
-- 13.1 Revenue Per Payment Method
SELECT 
    pm.payment_method,
    COUNT(*) AS transaction_count,
    SUM(t.total_amount_ngn) AS total_revenue
FROM sales.transactions t
JOIN ref.payment_methods pm ON t.payment_id = pm.payment_id
GROUP BY pm.payment_method
ORDER BY total_revenue DESC;

--13.2 Payment Methods with Discount Analysis
SELECT
    pm.payment_method,
    COUNT(*) AS transaction_count,
    SUM(t.total_amount_ngn) AS total_revenue,
    SUM(CASE WHEN t.discount_applied IN ('TRUE','True','1') THEN 1 ELSE 0 END) AS discounted_transactions,
    SUM(CASE WHEN t.discount_applied IN ('FALSE','False','0') THEN 1 ELSE 0 END) AS full_price_transactions
FROM sales.transactions t
JOIN ref.payment_methods pm ON t.payment_id = pm.payment_id
GROUP BY pm.payment_method
ORDER BY total_revenue DESC;

--13.3 Payment Trends Per Store
SELECT 
    rs.store_name,
    pm.payment_method,
    COUNT(st.transaction_id) AS total_transactions,
    SUM(st.total_amount_ngn) AS total_sales_amount
FROM sales.transactions AS st
INNER JOIN ref.stores AS rs
    ON st.store_id = rs.store_id
INNER JOIN ref.payment_methods AS pm
    ON st.payment_id = pm.payment_id
GROUP BY 
    rs.store_name,
    pm.payment_method
ORDER BY 
    rs.store_name,
    total_transactions DESC;
-- ======================================================================================================================================================================================================================================================================================================================
--14 CASHIER PERFORMANCE ANALYSIS
-- ======================================================================================================================================================================================================================================================================================================================
--14.1 Cashier Performace - Below Expectations Jan
DECLARE @TargetMeet DECIMAL(18,2) = 9000000;
DECLARE @SalesMonth VARCHAR(7) = '2024-01'; -- YYYY-MM, change as needed

WITH Monthly AS (
    SELECT
        UPPER(LTRIM(RTRIM(cashier_id))) AS cashier_id,   -- normalize id
        FORMAT(transaction_date, 'yyyy-MM') AS SalesMonth,
        SUM(items_count) AS TotalItems,
        SUM(total_amount_ngn) AS TotalSales
    FROM sales.transactions
    GROUP BY
        UPPER(LTRIM(RTRIM(cashier_id))),
        FORMAT(transaction_date, 'yyyy-MM')
)
SELECT
    cashier_id,
    SalesMonth,
    TotalItems,
    TotalSales
FROM Monthly
WHERE SalesMonth = @SalesMonth
  AND TotalSales < @TargetMeet    -- Below expectation (change operator if you want <=)
ORDER BY TotalSales DESC;


--14.2 Cashier Performanc - Beyond Expectation
GO

DECLARE @TargetMeet DECIMAL(18,2) = 9000000;
DECLARE @SalesMonth VARCHAR(7) = '2024-01';

WITH Monthly AS (
    SELECT
        UPPER(LTRIM(RTRIM(cashier_id))) AS cashier_id, 
        FORMAT(transaction_date, 'yyyy-MM') AS SalesMonth,
        SUM(items_count) AS TotalItems,
        SUM(total_amount_ngn) AS TotalSales
    FROM sales.transactions
    GROUP BY
        UPPER(LTRIM(RTRIM(cashier_id))),
        FORMAT(transaction_date, 'yyyy-MM')
)
SELECT
    cashier_id,
    SalesMonth,
    TotalItems,
    TotalSales
FROM Monthly
WHERE SalesMonth = @SalesMonth
  AND TotalSales > @TargetMeet  
ORDER BY TotalSales DESC;

-- ======================================================================================================================================================================================================================================================================================================================
--15. STORE PERFORMANCE ANALYSIS
-- ======================================================================================================================================================================================================================================================================================================================
GO
--15.1 Store Performance - January Balogun Market
DECLARE 
    @TargetMeet DECIMAL(18,2) = 9000000,
    @SalesMonth VARCHAR(7) = '2024-01',
    @StoreName NVARCHAR(100) = 'Balogun Market';

WITH Monthly AS (
    SELECT
        rs.store_name,
        FORMAT(st.transaction_date, 'yyyy-MM') AS SalesMonth,
        SUM(st.items_count) AS TotalItems,
        SUM(st.total_amount_ngn) AS TotalSales
    FROM sales.transactions AS st
    INNER JOIN ref.stores AS rs 
        ON st.store_id = rs.store_id
    GROUP BY
        rs.store_name,
        FORMAT(st.transaction_date, 'yyyy-MM')
)
SELECT
    store_name,
    SalesMonth,
    TotalItems,
    TotalSales,
    CASE
        WHEN TotalSales < @TargetMeet THEN 'Below Expectation'
        WHEN TotalSales BETWEEN @TargetMeet AND (@TargetMeet + 999999.99) THEN 'Meets Expectation'
        ELSE 'Beyond Expectation'
    END AS PerformanceStatus
FROM Monthly
WHERE 
    SalesMonth = @SalesMonth
    AND store_name = @StoreName
    AND TotalSales > @TargetMeet
ORDER BY TotalSales DESC;

--15.2 Store Monthly Performace - March
GO
DECLARE 
    @TargetMeet DECIMAL(18,2) = 9000000,
    @SalesMonth VARCHAR(7) = '2024-03';

WITH Monthly AS (
    SELECT
        rs.store_name,
        FORMAT(st.transaction_date, 'yyyy-MM') AS SalesMonth,
        SUM(st.items_count) AS TotalItems,
        SUM(st.total_amount_ngn) AS TotalSales
    FROM sales.transactions AS st
    INNER JOIN ref.stores AS rs 
        ON st.store_id = rs.store_id
    GROUP BY
        rs.store_name,
        FORMAT(st.transaction_date, 'yyyy-MM')
)
SELECT
    store_name,
    SalesMonth,
    TotalItems,
    TotalSales,
    CASE
        WHEN TotalSales < @TargetMeet THEN 'Below Expectation'
        WHEN TotalSales BETWEEN @TargetMeet AND (@TargetMeet + 999999.99) THEN 'Meets Expectation'
        ELSE 'Beyond Expectation'
    END AS PerformanceStatus
FROM Monthly
WHERE SalesMonth = @SalesMonth
ORDER BY TotalSales DESC;
-- ===========================================================================================================================================
-- 16. Populate Analytics Table
-- ===========================================================================================================================================
INSERT INTO analytics.daily_summary (
    report_date,
    total_transactions,
    total_revenue,
    transactions_with_discount,
    transactions_without_discount,
    total_loyalty_points,
    top_city,
    top_store
)
SELECT
    CAST(transaction_date AS DATE) AS report_date,
    COUNT(*) AS total_transactions,
    SUM(total_amount_ngn) AS total_revenue,
    SUM(CASE WHEN discount_applied IN ('TRUE','True','1') THEN 1 ELSE 0 END) AS transactions_with_discount,
    SUM(CASE WHEN discount_applied IN ('FALSE','False','0') THEN 1 ELSE 0 END) AS transactions_without_discount,
    SUM(loyalty_points_earned) AS total_loyalty_points,
    -- Top city per day
    (SELECT TOP 1 st.city
     FROM sales.transactions t2
     JOIN ref.stores st ON t2.store_id = st.store_id
     WHERE CAST(t2.transaction_date AS DATE) = CAST(t1.transaction_date AS DATE)
     GROUP BY st.city
     ORDER BY COUNT(*) DESC) AS top_city,
    -- Top store per day
    (SELECT TOP 1 st.store_name
     FROM sales.transactions t3
     JOIN ref.stores st ON t3.store_id = st.store_id
     WHERE CAST(t3.transaction_date AS DATE) = CAST(t1.transaction_date AS DATE)
     GROUP BY st.store_name
     ORDER BY COUNT(*) DESC) AS top_store
FROM sales.transactions t1
GROUP BY CAST(transaction_date AS DATE);

-- ======================================================================================================================================================================================================================================================================================================================
--17. REVENUE TREND
-- ======================================================================================================================================================================================================================================================================================================================
--17.1 Daily Revenue Trend
SELECT 
    transaction_date,
    SUM(total_amount_ngn) AS DailyTotal
FROM sales.transactions
GROUP BY transaction_date
ORDER BY transaction_date;
GO

SELECT 
    CAST(transaction_date AS DATE) AS transaction_date_only,
    SUM(total_amount_ngn) AS DailyTotal
FROM sales.transactions
GROUP BY CAST(transaction_date AS DATE)
ORDER BY transaction_date_only;
GO
--17.2 Weekly Revenue Trend
SELECT
    DATEPART(WEEK, transaction_date) AS WeekNumber,
    YEAR(transaction_date) AS Year,
    SUM(total_amount_ngn) AS WeeklyTotal
FROM sales.transactions
GROUP BY YEAR(transaction_date), DATEPART(WEEK, transaction_date)
ORDER BY Year, WeekNumber;
GO

--17.3 Monthly Revenue Trend
SELECT
    DATENAME(MONTH, transaction_date) AS Month,
    MONTH(transaction_date) AS MonthNumber,
    YEAR(transaction_date) AS Year,
    SUM(total_amount_ngn) AS MonthlyTotal
FROM sales.transactions
GROUP BY DATENAME(MONTH, transaction_date), MONTH(transaction_date), YEAR(transaction_date)
ORDER BY Year, MonthNumber;
GO

--17.4 Quarterly Revenue Trend
SELECT
    YEAR(transaction_date) AS Year,
    DATEPART(QUARTER, transaction_date) AS Quarter,
    SUM(total_amount_ngn) AS QuarterlyTotal
FROM sales.transactions
GROUP BY YEAR(transaction_date), DATEPART(QUARTER, transaction_date)
ORDER BY Year, Quarter;
GO

-- 17.5. Yearly Revenue Trend
SELECT
    YEAR(transaction_date) AS Year,
    SUM(total_amount_ngn) AS YearlyTotal
FROM sales.transactions
GROUP BY YEAR(transaction_date)
ORDER BY Year DESC;
GO
-- ===========================================================================================================================================
-- 18. Loyalty Performance
-- ===========================================================================================================================================
-- 18.1 Loyalty Points Overview
SELECT 
    SUM(loyalty_points_earned) AS total_loyalty_points,
    AVG(loyalty_points_earned) AS avg_points_per_transaction
FROM sales.transactions;

--Monthly Revenue Stored Procedure
GO
-- Monthly Revenue Stored Procedure
CREATE PROCEDURE analytics.sp_monthly_revenue
    @Year INT
AS
BEGIN
    SELECT 
        MONTH(transaction_date) AS MonthNumber,
        SUM(total_amount_ngn) AS TotalRevenue
    FROM sales.transactions
    WHERE YEAR(transaction_date) = @Year
    GROUP BY MONTH(transaction_date)
    ORDER BY MonthNumber;
END;

--Daily Summary
GO
CREATE PROCEDURE analytics.sp_load_daily_summary AS
BEGIN
    INSERT INTO analytics.daily_summary (report_date, total_transactions, total_revenue)
    SELECT 
        CAST(transaction_date AS DATE),
        COUNT(*),
        SUM(total_amount_ngn)
    FROM sales.transactions
    GROUP BY CAST(transaction_date AS DATE);
END;

--Revenue View
GO
CREATE VIEW analytics.vw_revenue_per_store AS
SELECT 
    st.store_name,
    SUM(t.total_amount_ngn) AS total_revenue
FROM sales.transactions t
JOIN ref.stores st ON t.store_id = st.store_id
GROUP BY st.store_name;

--Trending View
GO
CREATE VIEW analytics.vw_daily_revenue AS
SELECT 
    CAST(transaction_date AS DATE) AS report_date,
    SUM(total_amount_ngn) AS daily_revenue
FROM sales.transactions
GROUP BY CAST(transaction_date AS DATE);

--===============================================================================================================================
-- **END**
--===============================================================================================================================