--=========================================
--Customer table
--=========================================
SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	lo.cntry
FROM 
	silver.crm_cust_info ci
LEFT JOIN 
	silver.erp_cust_az12 ca
ON 
	ci.cst_key = ca.cid
LEFT JOIN 
	silver.erp_loc_a101 lo
ON 
	ci.cst_key = lo.cid

--After joining, check if any duplicate has been created.

SELECT 
	cst_id, 
	COUNT(*)
FROM
	(
	SELECT 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		lo.cntry
	FROM 
		silver.crm_cust_info ci
	LEFT JOIN 
		silver.erp_cust_az12 ca
	ON 
		ci.cst_key = ca.cid
	LEFT JOIN 
		silver.erp_loc_a101 lo
	ON 
		ci.cst_key = lo.cid
	)t
GROUP BY 
	cst_id
HAVING 
	COUNT(*) > 1

--Notice we have gender in both tables. Let us carryout data integration.
--Considering CRM as the most reliable source.

SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE 
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'n/a')
	END cst_gndr
FROM 
	silver.crm_cust_info ci
LEFT JOIN 
	silver.erp_cust_az12 ca
ON 
	ci.cst_key = ca.cid
LEFT JOIN 
	silver.erp_loc_a101 lo
ON 
	ci.cst_key = lo.cid
ORDER BY
	1,2;

--Final Transformation (rename and re-order columns, create surrogate key)
--Create view for new table

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	lo.cntry AS country,
	ci.cst_marital_status As marital_status,
	CASE 
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'n/a')
	END gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM 
	silver.crm_cust_info ci
LEFT JOIN 
	silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN 
	silver.erp_loc_a101 lo
ON ci.cst_key = lo.cid;

--=========================================
--Product table
--=========================================
SELECT 
	pr.prd_id,
	pr.cat_id,
	pr.prd_key,
	pr.prd_nm,
	pr.prd_cost,
	pr.prd_line,
	pr.prd_start_dt,
	pr.prd_end_dt,
	px.cat,
	px.subcat,
	px.maintenance
FROM 
	silver.crm_prd_info pr
LEFT JOIN 
	silver.erp_px_cat_g1v2 px
ON pr.cat_id = px.id
WHERE pr.prd_end_dt IS NULL; -- Filter out all historical data.


-- Check for duplicate after joining.
SELECT 
	prd_key,
	COUNT(*)
FROM
	(
	SELECT 
		pr.prd_id,
		pr.cat_id,
		pr.prd_key,
		pr.prd_nm,
		pr.prd_cost,
		pr.prd_line,
		pr.prd_start_dt,
		pr.prd_end_dt,
		px.cat,
		px.subcat,
		px.maintenance
	FROM 
		silver.crm_prd_info pr
	LEFT JOIN 
		silver.erp_px_cat_g1v2 px
	ON pr.cat_id = px.id
	WHERE pr.prd_end_dt IS NULL
	)t
GROUP BY 
	prd_key
HAVING 
	COUNT(*) > 1;

--Rearrange by grouping matching columns together.
--Rename columns.
--create surrogate key for dim table.

SELECT 
	ROW_NUMBER() OVER(ORDER BY pr.prd_start_dt, pr.prd_key) AS product_key,
	pr.prd_id AS product_id,
	pr.prd_key AS product_number,
	pr.prd_nm AS product_name,
	pr.cat_id AS category_id,
	px.cat AS category,
	px.subcat AS subcategory,
	px.maintenance,
	pr.prd_cost AS product_cost,
	pr.prd_line AS product_line,
	pr.prd_start_dt AS start_date
FROM 
	silver.crm_prd_info pr
LEFT JOIN 
	silver.erp_px_cat_g1v2 px
ON pr.cat_id = px.id
WHERE pr.prd_end_dt IS NULL;

--=========================================
--Sales & Orders table
--=========================================
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM 
	silver.crm_sales_details sd
LEFT JOIN
	gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN 
	gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;