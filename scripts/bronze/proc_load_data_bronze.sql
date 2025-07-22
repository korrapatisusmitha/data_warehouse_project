/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze;
===============================================================================
*/
create or replace procedure bronze.load_bronze()
language plpgsql
as $$
declare
	batch_start_time timestamp;
	batch_end_time timestamp;
	start_time timestamp;
	end_time timestamp;
	duration interval;
begin
		batch_start_time := clock_timestamp();
	begin
		raise notice '=========================';
		raise notice 'Loading Bronze Layer';
		raise notice '=========================';

		raise notice '-------------------------';
		raise notice 'Loading CRM Tables';
		raise notice '-------------------------';
		
		start_time := clock_timestamp();
		raise notice 'Truncating table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
		raise notice 'Inserting data into: bronze.crm_cust_info';	
		copy bronze.crm_cust_info
		from 'F:\sql_data_warehouse_project\datasets\source_crm\cust_info.csv'
		with(
			FORMAT csv,
			HEADER true,
			DELIMITER ','
		);
		end_time := clock_timestamp();
		duration := end_time - start_time;
		raise notice 'Duration: % msec', duration;
		raise notice '<--------->';
		
		start_time := clock_timestamp();
		raise notice 'Truncating table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
		raise notice 'Inserting data into: bronze.crm_prd_info';
		copy bronze.crm_prd_info
		from 'F:\sql_data_warehouse_project\datasets\source_crm\prd_info.csv'
		with(
			FORMAT csv,
			HEADER true,
			DELIMITER ','
		);
		end_time := clock_timestamp();
		duration := end_time - start_time;
		raise notice 'Duration: % msec', duration;
		raise notice '<--------->';

		start_time := clock_timestamp();	
		raise notice 'Truncating table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		raise notice 'Inserting Data into: bronze.crm_sales_details';
		copy bronze.crm_sales_details
		from 'F:\sql_data_warehouse_project\datasets\source_crm\sales_details.csv'
		with(
			FORMAT csv,
			HEADER true,
			DELIMITER ','
		);
		end_time := clock_timestamp();
		duration := end_time - start_time;
		raise notice 'Duration: % msec', duration;
		raise notice '<--------->';

		raise notice '-------------------------';
		raise notice 'Loading ERP Tables';
		raise notice '-------------------------';

		start_time := clock_timestamp();
		raise notice 'Truncating table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;
		raise notice 'Inserting Data into: bronze.erp_cust_az12';	
		copy bronze.erp_cust_az12
		from 'F:\sql_data_warehouse_project\datasets\source_erp\cust_az12.csv'
		with(
			FORMAT csv,
			HEADER true,
			DELIMITER ','
		);
		end_time := clock_timestamp();
		duration := end_time - start_time;
		raise notice 'Duration: % msec', duration;
		raise notice '<--------->';

		start_time := clock_timestamp();
		raise notice 'Truncating table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;
		raise notice 'Inserting Data into: bronze.erp_loc_a101';
		copy bronze.erp_loc_a101
		from 'F:\sql_data_warehouse_project\datasets\source_erp\loc_a101.csv'
		with(
			FORMAT csv,
			HEADER true,
			DELIMITER ','
		);
		end_time := clock_timestamp();
		duration := end_time - start_time;
		raise notice 'Duration: % msec', duration;
		raise notice '<--------->';

		start_time := clock_timestamp();
		raise notice 'Truncating table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		raise notice 'Inserting Data into: bronze.erp_px_cat_g1v2';
		copy bronze.erp_px_cat_g1v2
		from 'F:\sql_data_warehouse_project\datasets\source_erp\px_cat_g1v2.csv'
		with(
			FORMAT csv,
			HEADER true,
			DELIMITER ','
		);
		end_time := clock_timestamp();
		duration := end_time - start_time;
		raise notice 'Duration: % msec', duration;
		raise notice '<--------->';
		
		batch_end_time := clock_timestamp();
		duration := batch_end_time - batch_start_time ;
		raise notice '<=========>';
		raise notice 'Loading Bronze Layer is Completed!';
		raise notice '	- Total Duration: % msec', duration;
		raise notice '<=========>';

	exception
		when others then
			raise notice 'An error occurred!';
			raise notice 'Error Message: %', SQLERRM;
			raise notice 'SQLSTATE: %', SQLSTATE;
	end;
end;
$$;

call bronze.load_bronze();