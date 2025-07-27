/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
-- Quality checks
-- Check for uniqueness of customer key
-- Expectation : No Results
select cst_id, count(*)
from
(select
	cu.cst_id,
	cu.cst_key,
	cu.cst_firstname,
	cu.cst_lastname,
	cu.cst_marital_status,
	cu.cst_gndr,
	cu.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
from silver.crm_cust_info cu
left join silver.erp_cust_az12 ca
on cu.cst_key = ca.cid
left join silver.erp_loc_a101 la
on cu.cst_key = la.cid) t
group by cst_id
having count(*) > 1;

-- Check for data integration
select distinct
	cu.cst_gndr,
	ca.gen,
	case when cu.cst_gndr != 'n/a' then cu.cst_gndr -- CRM is the master of the gender Info
		else coalesce(ca.gen, 'n/a')
	end as new_gen
from silver.crm_cust_info cu
left join silver.erp_cust_az12 ca
on cu.cst_key = ca.cid
left join silver.erp_loc_a101 la
on cu.cst_key = la.cid
order by 1, 2;

select * from gold.dim_customers;

select distinct gender from gold.dim_customers;
-- <==============================>
-- Check for uniqueness of product_key
-- Expectations: NO Results
select prd_key, count(*) from
(select 
	pd.prd_id,
	pd.cat_id,
	pc.cat,
	pc.subcat,
	pc.maintenance,
	pd.prd_nm,
	pd.prd_key,
	pd.prd_cost,
	pd.prd_line, 
	pd.prd_start_dt
from silver.crm_prd_info pd
left join silver.erp_px_cat_g1v2 pc
on pd.cat_id = pc.id
where prd_end_dt is null) --filter out all historical data
 group by prd_key
 having count(*) > 1
;

select * from gold.dim_products;
-- <==============================>
select * from gold.fact_sales;

-- Fact check: check if all dimension tables can successfully join the fact table
-- Foreign key Integrity (Dimensions)
select 	*
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
where c.customer_key is null or p.product_key is null;
