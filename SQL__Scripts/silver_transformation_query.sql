/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/


--========================================================
--Data Transformation Query for silver.crm_cust_info
--========================================================
SELECT *
FROM bronze.crm_cust_info;

--Check for nulls and duplicates in primary key.
--Expectation: There should be no result.
SELECT 
	cst_id,
	COUNT(cst_id) duplicate
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(cst_id) >1 or cst_id IS NULL;


--Remove duplicates and null values from data

SELECT *
FROM
	(
	SELECT 
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	) t
WHERE flag_last = 1 
	AND
	cst_id IS NOT NULL;

--Check for unwanted spacing
--Expectation: There should be no result.

SELECT 
	cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT 
	cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT 
	cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

SELECT 
	cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

--Let us fix this

SELECT 
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname
FROM bronze.crm_cust_info;

--Check for data standardization & consistency.

SELECT 
	DISTINCT cst_marital_status
FROM bronze.crm_cust_info;

SELECT 
	DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT 
	cst_marital_status,
	cst_gndr,
	CASE
		WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
		WHEN UPPER(cst_marital_status)s = 'S' THEN 'Single'
		ELSE 'n/a'
	END cst_marital_status,
	CASE
		WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
		WHEN UPPER(cst_gndr)  = 'F' THEN 'Female'
		ELSE 'n/a'
	END cst_gndr
FROM bronze.crm_cust_info;

--For dates, make sure it is date and not string.

-- NOVA ACADEMY Final Transformation for silver.crm_cust_info

SELECT 
	cst_id,
	cst_key,
    TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
    CASE
		WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
		WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
		ELSE 'n/a'
	END cst_marital_status,
	CASE
		WHEN UPPER(cst_gndr)  = 'M' THEN 'Male'
		WHEN UPPER(cst_gndr)  = 'F' THEN 'Female'
		ELSE 'n/a'
	END cst_gndr,
    cst_create_date
FROM
	(
	SELECT 
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	) t
WHERE flag_last = 1 
	AND
	cst_id IS NOT NULL;


--========================================================
--Data Transformation Query for silver.crm_prd_info
--========================================================
SELECT *
FROM bronze.crm_prd_info

--Check for nulls and duplicates in primary key.
--Expectation: There should be no result.
SELECT 
	prd_id,
	COUNT(prd_id) duplicate
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(prd_id) >1 or prd_id IS NULL;


-- Extract cat_id from prd_key (First 5 characters) and replace "-" with "_"
-- Extract prd_key from prd_key (Remaining characters in prd_key)
SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM 
	bronze.crm_prd_info;


--Check for unwanted spacing
--Expectation: There should be no result.

SELECT 
	prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


--Check for negative price and null values.
SELECT 
	prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR	
	  prd_cost IS NULL
--Fix
SELECT 
	ISNULL(prd_cost, 0) AS prd_cost
FROM bronze.crm_prd_info

--Check for data standardization & consistency.

SELECT 
	DISTINCT prd_line
FROM bronze.crm_prd_info;

SELECT 
	prd_line,
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line
FROM bronze.crm_prd_info;

--Check for invalid date order. The end of current/previous order must be smaller than the start of current/next order.
--Start date comes from end date of previous order.

SELECT 
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	prd_cost
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

--Fix
SELECT 
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM 
	bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

-- NOVA ACADEMY Final Transformation for silver.crm_prd_info

SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM 
	bronze.crm_prd_info;


--========================================================
--Data Transformation Query for silver.crm_sales_details
--========================================================
SELECT *
FROM bronze.crm_sales_details

--Check for unwanted spacing
--Expectation: There should be no result.
SELECT 
	sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

--Check to ensure matching keys from other tables.
SELECT 
    sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

SELECT 
    sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

--Check for invalid dates
SELECT 
	sls_order_dt, 
	sls_ship_dt,
	sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 

--Fix
SELECT 
	NULLIF(sls_order_dt, 0) AS sls_order_dt, 
	sls_ship_dt,
	sls_due_dt
FROM bronze.crm_sales_details

--Check for irregular date length (Must be = 8)
SELECT 
	NULLIF(sls_order_dt, 0) AS sls_order_dt,  
	sls_ship_dt,
	sls_due_dt
FROM bronze.crm_sales_details
WHERE 
	sls_order_dt <= 0 
	OR LEN(sls_order_dt)!= 8
	OR sls_order_dt > 20500101
	OR sls_order_dt < 20000101

-- Fix
SELECT 
	CASE 
		WHEN sls_order_dt <= 0 OR LEN(sls_order_dt)!= 8 THEN NULL
		ELSE sls_order_dt
	END AS sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM bronze.crm_sales_details

--Allocate appropriate date type.
SELECT 
	CASE 
		WHEN sls_order_dt <= 0 OR LEN(sls_order_dt)!= 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) AS sls_ship_dt,
	CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) AS sls_due_dt
FROM bronze.crm_sales_details

--Ensure order date < shipping and due date
SELECT 
	CASE 
		WHEN sls_order_dt <= 0 OR LEN(sls_order_dt)!= 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) AS sls_ship_dt,
	CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE 
	sls_order_dt > sls_ship_dt
	OR sls_order_dt > sls_due_dt

--Ensure that sales = quantity * price, also check for negative, zero and null sales values. 
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE 
	sls_sales != sls_quantity * sls_price
	OR sls_sales <= 0 	
	OR sls_quantity <= 0 
	OR sls_price <= 0
	OR  sls_sales IS NULL 
	OR sls_quantity IS NULL 
	OR sls_price IS NULL
ORDER BY
	sls_sales,
	sls_quantity,
	sls_price

/*
Transformation Rules (FIX)
1. If sales is negative, zero, or null, derive it using Quantity and Price
2. If price is zero or null, calculate it using Sales and Quantity.
3. If price is negative, convert it to positive.
*/

SELECT 
	CASE
		WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	CASE 
		WHEN sls_price <= 0 OR sls_price IS NULL 
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details


-- NOVA ACADEMY Final Transformation for silver.crm_sales_details

SELECT 
	sls_ord_num,
    sls_prd_key,
	sls_cust_id,
    CASE 
		WHEN sls_order_dt <= 0 OR LEN(sls_order_dt)!= 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt)!= 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt <= 0 OR LEN(sls_due_dt)!= 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
    CASE
		WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE 
		WHEN sls_price <= 0 OR sls_price IS NULL 
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details;


--========================================================
--Data Transformation Query for silver.erp_cust_az12
--========================================================
SELECT *
FROM bronze.erp_cust_az12

--Check for nulls and duplicates in primary key.
--Expectation: There should be no result.
SELECT 
	cid,
	COUNT(cid) duplicate
FROM bronze.erp_cust_az12
GROUP BY cid
HAVING COUNT(cid) >1 or cid IS NULL;

--Check for unwanted spacing
--Expectation: There should be no result.
SELECT 
	cid
FROM bronze.erp_cust_az12
WHERE cid != TRIM(cid)

-- Extract cat_id from cid (Remove first 3 characters)
SELECT 
	cid,
	CASE
		WHEN cid LIKE '%NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid
FROM bronze.erp_cust_az12


--Check to ensure matching keys from other tables.
SELECT 
	CASE
		WHEN cid LIKE '%NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid
FROM bronze.erp_cust_az12
WHERE 
	CASE
		WHEN cid LIKE '%NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END
	NOT IN (SELECT cst_key FROM silver.crm_cust_info);

--Check for and Remove leading and trailing spaces from bdate if any.

SELECT DISTINCT
	bdate
FROM 
	bronze.erp_cust_az12
WHERE
	bdate < '1925-01-01'
	OR bdate > GETDATE()

--Fix
SELECT 
	bdate,
	CASE
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate
FROM 
	bronze.erp_cust_az12

--Check for data standardization & consistency for Gen column

SELECT DISTINCT
	gen,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12


-- NOVA ACADEMY Final Transformation for silver.erp_cust_az12
SELECT 
	CASE
		WHEN cid LIKE '%NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid,
	CASE
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;


--========================================================
--Data Transformation Query for silver.erp_loc_a101
--========================================================
SELECT *
FROM bronze.erp_loc_a101;

--Check for nulls and duplicates in primary key.
--Expectation: There should be no result.
SELECT 
	cid,
	COUNT(cid) duplicate
FROM bronze.erp_loc_a101
GROUP BY cid
HAVING COUNT(cid) >1 or cid IS NULL;

--Check for unwanted spacing
--Expectation: There should be no result.
SELECT 
	cid
FROM bronze.erp_loc_a101
WHERE cid != TRIM(cid);

--Replace "-" in cid with ""
SELECT 
	cid,
	REPLACE(cid, '-', '') AS cid
FROM bronze.erp_loc_a101;

--Check to ensure matching keys from other tables.
SELECT 
	REPLACE(cid, '-', '') AS cid
FROM bronze.erp_loc_a101
WHERE 
	REPLACE(cid, '-', '')
	NOT IN (SELECT cst_key FROM silver.crm_cust_info);

--Check for data standardization & consistency for Gen column

SELECT DISTINCT
	cntry,
	CASE
		WHEN UPPER(TRIM(cntry)) IN ('DE') THEN 'Germany' 
		WHEN UPPER(TRIM(cntry)) IN ('USA', 'US') THEN 'United States' 
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;


-- NOVA ACADEMY Final Transformation for silver.erp_loc_a101
SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE
		WHEN UPPER(TRIM(cntry)) IN ('DE') THEN 'Germany' 
		WHEN UPPER(TRIM(cntry)) IN ('USA', 'US') THEN 'United States' 
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;


--========================================================
--Data Transformation Query for silver.erp_px_cat_g1v2
--========================================================
SELECT *
FROM bronze.erp_px_cat_g1v2;

--Check for nulls and duplicates in primary key.
--Expectation: There should be no result.
SELECT 
	id,
	COUNT(id) duplicate
FROM bronze.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(id) >1 or id IS NULL;

--Check for unwanted spacing
--Expectation: There should be no result.
SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2
WHERE id != TRIM(id)
	OR cat != TRIM(cat)
	OR subcat != TRIM(subcat)
	OR maintenance != TRIM(maintenance);


--Check for data standardization & consistency for Gen column

SELECT DISTINCT
	maintenance
FROM bronze.erp_px_cat_g1v2


-- NOVA ACADEMY Final Transformation for silver.erp_px_cat_g1v2
SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2;