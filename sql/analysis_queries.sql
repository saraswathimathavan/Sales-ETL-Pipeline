-- 1. View all available tables
SHOW TABLES IN sales_database;

-- 2. Check sales fact table
SELECT *
FROM sales_database.fact_sales
LIMIT 10;

-- 3. Check product dimension table
SELECT *
FROM sales_database.dim_product
LIMIT 10;

-- 4. Count sales records
SELECT COUNT(*) AS total_records
FROM sales_database.fact_sales;
