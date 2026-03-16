/*
==============================================================
stored procedure: load silver layer (bronze -> silver)
==============================================================
script purpose:
	this stored procedure performs the ETL (extract, transform, load) process to 
	populate the 'silver' schema tables form the 'bronze' schema.
action performed:
	- truncates silver tables.
	-inserts transformed and cleansed data from bronze into silver tables.

parameters:
	none.
	this stored procedure does not accept any parameters or return any values.


usage example:
	exec silver.load_silver;
========================================================================
*/

exec silver.load_silver

create or alter procedure silver.load_silver as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime,@batch_end_time datetime;
	begin try
			set @batch_start_time = getdate();
			print'========================================'
			print'loading bronze layer';
			print'========================================'
	
	
			print'----------------------------------------'
			print'loading crm tables';
			print'----------------------------------------'
	
			set @start_time = getdate();
			print'>> Truncating table: silver.crm_cust_info';
			truncate table silver.crm_cust_info;
			print '>> inserting data info: silver.crm_cust_info';
			insert into silver.crm_cust_info(
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
			)

			select
				cst_id,
				cst_key,
				trim(cst_firstname) as cst_firstname,
				trim(cst_lastname) as cst_lastname,
				case when cst_marital_status = 's' then 'single'
					when cst_marital_status = 'm'then 'married'
					else 'n/a'
				end cst_marital_status,
				case when cst_gndr = 'f' then 'female'
					 when cst_gndr = 'm'then 'male'
					 else 'n/a'
				end cst_gndr,
				cst_create_date
			from(
				select 
					*,
					row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
					from bronze.crm_cust_info
					where cst_id is not null
				)t 
				where flag_last = 1 ;
				SET @end_time = GETDATE();
				print'>>load duration:' + cast(datediff(second,@start_time, @end_time)as nvarchar) + 'seconds';
				print'>> ----------------';


			set @start_time = getdate();
			print'>> Truncating table: silver.crm_prd_info';
			truncate table silver.crm_prd_info;
			print '>> inserting data info: silver.crm_prd_info';

			insert into silver.crm_prd_info(
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
			)
			select
			prd_id,
			replace(substring(prd_key, 1,5), '-', '_') as cat_id,
			substring(prd_key, 7, len(prd_key)) as prd_key,
			prd_nm,
			isnull(prd_cost, 0) as prd_cost,
			case upper(trim(prd_line)) 
				 when 'm' then 'mountain'
				 when 'r' then 'road'
				 when 's' then 'other sales'
				 when 't' then 'touring'
				 else 'n/a'
			end as prd_line,
			cast(prd_start_dt as date) as prd_start_dt,
			cast(LEAD(prd_start_dt) OVER (PARTITION by prd_key ORDER BY prd_start_dt)-1 as date) as prd_end_dt
			from bronze.crm_prd_info;
			SET @end_time = GETDATE();
			print'>>load duration:' + cast(datediff(second,@start_time, @end_time)as nvarchar) + 'seconds';
			print'>> ----------------';


			set @start_time = getdate();
			print'>> Truncating table: silver.crm_sales_details';
			truncate table silver.crm_sales_details;
			print '>> inserting data info: silver.crm_sales_details';


			insert into silver.crm_sales_details(
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price   
			)

			select
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
					else cast(cast (sls_order_dt as varchar) as date)
				end as sls_order_dt,
				case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
					else cast(cast (sls_ship_dt as varchar) as date)
				end as sls_ship_dt,
				case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
					else cast(cast (sls_due_dt as varchar) as date)
				end as sls_due_dt,

				case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
						then sls_quantity *abs(sls_price)
					else sls_sales
				end as sls_sales,
				sls_quantity,
				case when sls_price is null or sls_price <= 0 
						then sls_sales / nullif(sls_quantity, 0)
					else sls_price
				end sls_price
			from bronze.crm_sales_details;
			SET @end_time = GETDATE();
			print'>>load duration:' + cast(datediff(second,@start_time, @end_time)as nvarchar) + 'seconds';
			print'>> ----------------';


			SET @start_time = GETDATE();
			print'>> Truncating table: silver.erp_cust_az12';
			truncate table silver.erp_cust_az12;
			print '>> inserting data info: silver.erp_cust_az12';

			insert into silver.erp_cust_az12(
				cid, 
				bdate, 
				gen
			)
			select
				CASE WHEN cid like 'NAS%' then substring(cid, 4, len(cid))
					else cid
				end as cid,
				case when bdate > getdate() then null
					else bdate
				end as bdate,
				case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
					 when upper(trim(gen)) in ('M', 'MALE') then 'Male'
					 else 'n/a'
				end as gen
			from bronze.erp_cust_az12;
			SET @end_time = GETDATE();
			print'>>load duration:' + cast(datediff(second,@start_time, @end_time)as nvarchar) + 'seconds';
			print'>> ----------------';

			
		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

			SET @start_time = GETDATE();
			print'>> Truncating table: silver.erp_loc_a101';
			truncate table silver.erp_loc_a101;
			print '>> inserting data info: silver.erp_loc_a101';


			insert into silver.erp_loc_a101
			(cid, cntry)

			select
				replace(cid, '-', '')cid,
				case when trim(cntry) = 'DE' then 'Germany'
					 when trim(cntry) in ('US', 'USA') then 'United States'
					 when trim(cntry) = '' or cntry is null then 'n/a'
					 else trim(cntry)
				end as cntry
			from bronze.erp_loc_a101;
			SET @end_time = GETDATE();
			print'>>load duration:' + cast(datediff(second,@start_time, @end_time)as nvarchar) + 'seconds';
			print'>> ----------------';

			SET @start_time = GETDATE();
			print'>> Truncating table: silver.erp_px_cat_g1v2';
			truncate table silver.erp_px_cat_g1v2;
			print '>> inserting data info: silver.erp_px_cat_g1v2';


			insert into silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
			select 
				id, 
				cat,
				subcat,
				maintenance
			from bronze.erp_px_cat_g1v2;
			set @end_time = getdate();
			print'>>load duration:' + cast(datediff(second,@start_time, @end_time)as nvarchar) + 'seconds';
			print'>> ----------------';

			set @batch_end_time = getdate();
			print'================================='
			print'loading bronze layer is completed';
			print'  - total load duration:' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
			print'================================='
		end try
		begin catch
			print'================================='
			print'error occured during loading bronze layer'
			print'error message' + error_message();
			print'error message' + cast(error_number() as nvarchar);
			print'error message' + cast(error_state() as nvarchar);
			print'================================='
	end catch
end
