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

-- <<---- silver.crm_cust_info ---->
-- check for null or duplicates in the primary key(A primary key must be unique and not null)
-- Expectation: No results
select
	cst_id,
	count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null ;

-- check for unwanted spaces
-- Expectation: No results
select
	cst_firstname
from silver.crm_cust_info
where cst_firstname!= trim(cst_firstname);

select
	cst_lastname
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname);

select
	cst_gndr
from silver.crm_cust_info
where cst_gndr != trim(cst_gndr);

select
	cst_marital_status
from silver.crm_cust_info
where cst_marital_status != trim(cst_marital_status);

-- Data standardization & consistency  
select
	distinct cst_gndr
from silver.crm_cust_info;

select
	distinct cst_marital_status
from silver.crm_cust_info;

-- ======================================================
-- <<---- silver.crm_prd_info ---->
-- check for null or duplicates in the primary key(A primary key must be unique and not null)
-- Expectation: No results
select
	prd_id,
	count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null ;

-- select
-- 	prd_key,
-- 	count(*)
-- from silver.crm_prd_info
-- group by prd_key
-- having count(*) > 1 or prd_key is null ;

-- check for unwanted spaces
-- Expectation: No results
select
	prd_nm
from silver.crm_prd_info
where prd_nm!= trim(prd_nm);

-- Check for nulls or negative numbers
-- Expectations: No results
select
	prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- Data standardization & consistency  
select
	distinct prd_line
from silver.crm_prd_info;

-- check for invalid dates
-- End date must not be earlier than the start date
select
	*
from silver.crm_prd_info
where prd_end_dt < prd_start_dt;
-- select * from silver.crm_prd_info where prd_key = 'FR-R72Y-42';
-- ======================================================
-- <<---- silver.crm_sales_details ---->
-- check for unwanted spaces
select
* from silver.crm_sales_details
where sls_ord_num != trim(sls_ord_num);

-- check for invalid order dates
select
*
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

-- check data consistency: between slaes, quantity & price
-- >> sales = quantity 8 price
-- >> values must not be nulls, zeros and negative.
select
sls_sales ,
sls_quantity,
sls_price 
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <=0 or sls_price <=0
order by sls_sales, sls_quantity, sls_price;

select * from silver.crm_sales_details;
-- ======================================================
-- <<---- silver.erp_cust_az12 ---->
-- check for invalid dates
select distinct bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > now();

-- data standardization & normalization
select distinct gen
from silver.erp_cust_az12;

select * from silver.erp_cust_az12;
-- ======================================================
-- <<---- silver.erp_loc_a101 ---->
-- Data standardization & consistency
select
	distinct cntry 
from silver.erp_loc_a101
order by cntry;

select * from silver.erp_loc_a101;
-- ======================================================
-- <<---- silver.erp_px_cat_g1v2 ---->
-- check for unwanted spaces
select * from silver.erp_px_cat_g1v2
where id != trim(id)
or cat != trim(cat) 
or subcat != trim(subcat)
or maintenance != trim(maintenance);

-- Data satndardization & normalization
select distinct cat from silver.erp_px_cat_g1v2;
select distinct subcat from silver.erp_px_cat_g1v2;
select distinct maintenance from silver.erp_px_cat_g1v2;

select * from silver.erp_px_cat_g1v2;
