/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- Create Dimension: gold.dim_customers
create view gold.dim_customers as
select
	row_number() over(order by cst_id) as customer_key,
	cu.cst_id as customer_id,
	cu.cst_key as customer_number,
	cu.cst_firstname as first_name,
	cu.cst_lastname as last_name,
	la.cntry as country,
	cu.cst_marital_status as marital_status,
	case when cu.cst_gndr != 'n/a' then cu.cst_gndr -- CRM is the master of the gender Info
		else coalesce(ca.gen, 'n/a')
	end as gender,
	ca.bdate as birthdate,
	cu.cst_create_date as create_date
from silver.crm_cust_info cu
left join silver.erp_cust_az12 ca
on cu.cst_key = ca.cid
left join silver.erp_loc_a101 la
on cu.cst_key = la.cid;

-- <------------------------------------------------------->
-- Create Dimension: gold.dim_products
create view gold.dim_products as
select 
	row_number() over(order by pd.prd_start_dt, pd.prd_key) as product_key,
	pd.prd_id as product_id,
	pd.prd_key as product_number,
	pd.prd_nm as product_name,
	pd.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance ,
	pd.prd_cost as cost,
	pd.prd_line as product_line, 
	pd.prd_start_dt as start_date
from silver.crm_prd_info pd
left join silver.erp_px_cat_g1v2 pc
on pd.cat_id = pc.id
where prd_end_dt is null; --filter out all historical data

-- <------------------------------------------------------->
-- Create Dimension: gold_fact_sales
create view gold.fact_Sales as
select
	sd.sls_ord_num as order_number,
	pr.product_key ,
	cs.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cs
on sd.sls_cust_id = cs.customer_id;