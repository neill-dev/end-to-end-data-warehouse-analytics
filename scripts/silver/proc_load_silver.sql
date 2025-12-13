/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/


Create or alter procedure silver.load_silver as                                                             --1. this logic uses to create stored procedure so we can                                                   --
Begin                                                                                                        -- load data direct by using EXEC silver.load_silver
    Declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	  begin try
	     set @batch_start_time = GETDATE();
         print '==============================================';
         print 'Loading Silver Layer';
		 print '==============================================';

		 print'-----------------------------------------------';
		 print'Loading CRM Tables';
		 print'-----------------------------------------------';

		 --Loading silver.crm_cust_info
		 set @start_time = GETDATE();
	print '>> Truncating table: Silver.crm_cust_info';                                                      --  to avoid duplications if we run query twice it duplicates data
	Truncate table silver.crm_cust_info;
	print '>>Inserting Data into Silver.crm_cust_info';
	Insert into silver.crm_cust_info(
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
	Case When UPPER(trim(cst_material_status)) = 'S' Then 'Single'
		 when UPPER(trim(cst_material_status)) = 'M' Then 'Married'
		 else'n/a'
	end cst_material_status,

	Case when upper(Trim(cst_gndr)) = 'F' Then 'Female'  
		 when UPPER(Trim(cst_gndr)) = 'M' Then 'Male'    
		 else 'n/a'                                      
	End   cst_gndr,                                                           
	cst_create_date                                        
	from (

	select 
	*,
	row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info

	) t where flag_last = 1;
	 set @end_time = GETDATE();
		 print'>> Load duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + 'seconds';
		 print '--------------------------';



		 
		 set @start_time = GETDATE();
		
	print '>> Truncating table:silver.crm_prd_info'; 
	Truncate table silver.crm_prd_info;
	print '>>Inserting Data into silver.crm_prd_info';

	INSERT INTO silver.crm_prd_info(
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
	replace(SUBSTRING(prd_key,1,5), '-','_') as cat_id,
	SUBSTRING(prd_key,7 ,LEN(prd_key))   as prd_key,                               
                                                     				                                     
	prd_nm,                                            
	isnull(prd_cost,0) as prd_cost,                        
	case when upper(trim(prd_line)) = 'M' Then 'Mountain'
		 when upper(trim(prd_line)) = 'R' Then 'Road'
		 when upper(trim(prd_line)) = 'S' Then 'Other Sales'
		 when upper(trim(prd_line)) = 'T' Then 'Touring'
		 else 'n/a'
	end prd_line,
	cast(prd_start_dt  as date) as prd_start_dt,   
	cast(lead(prd_start_dt) over ( partition by prd_key order by prd_start_dt) - 1 as date) as prd_end_dt

	from bronze.crm_prd_info ;

	 set @end_time = GETDATE();
		 print'>> Load duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + 'seconds';
		 print '--------------------------';
		  

		 set @start_time = GETDATE();

	print '>> Truncating table:silver.crm_sales_details'; 
	Truncate table silver.crm_sales_details;
	print '>>Inserting Data into silver.crm_sales_details';
	INSERT INTO  silver.crm_sales_details(
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
	case when sls_order_dt = 0 or len(sls_order_dt) != 8 Then null
		   else cast(cast(sls_order_dt as varchar) as date) 
	END as sls_order_dt,                                                
                                                                    
	case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 Then null         
			   else cast(cast(sls_ship_dt as varchar) as date)                         
	END as sls_ship_dt, 

	case when sls_due_dt = 0 or len(sls_due_dt) != 8 Then null
	else cast(cast(sls_due_dt as varchar) as date) 
	END as sls_due_dt, 
                                     
	case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
		 Then sls_quantity * abs(sls_price) 
		 else sls_sales
	end as sls_sales,
	sls_quantity,
	case when sls_price is null or sls_price < = 0 
		Then sls_sales / nullif(sls_quantity,0)
		   else sls_price 
	End as sls_price

	from bronze.crm_sales_details ;
	set @end_time = GETDATE();
		 print'>> Load duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + 'seconds';
		 print '--------------------------';
		  

	 set @start_time = GETDATE();
	print '>> Truncating table: silver.erp_cust_az12'; 
	Truncate table  silver.erp_cust_az12;
	print '>>Inserting Data into  silver.erp_cust_az12';

	INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen )

	select 
	case when cid like 'NAS%' then substring(cid,4,len(cid))
			 else cid
	end  as cid,

	case when bdate > Getdate() then null
	   else bdate
	end as bdate,

	case when upper(trim(gen)) in ('F', 'Female' ) Then 'Female'
		 when upper(trim(gen)) in ('M', 'Male') Then 'Male'
		 else'n/a'
	end as gen
	from bronze.erp_cust_az12;
	set @end_time = GETDATE();
		 print'>> Load duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + 'seconds';
		 print '--------------------------';


	 set @start_time = GETDATE();
	print '>> Truncating table: silver.erp_loc_a101'; 
	Truncate table  silver.erp_loc_a101;
	print '>>Inserting Data into  silver.erp_loc_a101';

	INSERT INTO silver.erp_loc_a101
	( cid,
	cntry)

	select 
	replace(cid,'-','') cid,
	case when trim(cntry) = 'DE' Then 'Germany'
		  when trim(cntry) in ('US','USA') then 'United states'
		  when trim(cntry) = '' or cntry is null then 'n/a'
		  else trim(cntry)
	end as cntry
	from bronze.erp_loc_a101 ;

	set @end_time = GETDATE();
		 print'>> Load duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + 'seconds';
		 print '--------------------------';
		  

	 set @start_time = GETDATE();
	print '>> Truncating table:  silver.erp_px_cat_g1v2'; 
	Truncate table   silver.erp_px_cat_g1v2;
	print '>>Inserting Data into   silver.erp_px_cat_g1v2';

	INSERT INTO silver.erp_px_cat_g1v2
	(id,
	cat,
	subcat,
	maintenance)

	select
	id,
	cat,
	subcat,
	maintenance
	from bronze.erp_px_cat_g1v2;
set @end_time = GETDATE();
		      print'>> Load duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + 'seconds';
		       print '--------------------------';
			   
			   set @batch_end_time = GETDATE();
			   print'=======================================';
			   print'Loading silver layer is completed';
			   print'Total load duration :' + cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) + 'seconds';
			   print'=======================================';

	end try
	begin catch
	   Print '===========================================';
	   print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
	   print 'Error Message'+ Error_Message();
	   print'Error Message' +cast(Error_number() as nvarchar);
	   print'Error Message' + cast(error_state() as nvarchar);
	   Print '===========================================';
		  
	   
	end catch

end
