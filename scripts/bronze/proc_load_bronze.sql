/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

create or alter procedure bronze.load_bronze as 
begin 
   Declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
	     set @batch_start_time = GETDATE();
         print '==============================================';
         print 'Loading Bronze Layer';
		 print '==============================================';

		 print'-----------------------------------------------';
		 print'Loading CRM Tables';
		 print'-----------------------------------------------';


		 set @start_time = GETDATE();
		 print'>> Truncating Table: bronze.crm_cust_info';
		 truncate table bronze.crm_cust_info;

         print'>> Inserting data into : Bronze.crm_cust_info';

		Bulk insert bronze.crm_cust_info
		from 'C:\Users\5425\Desktop\SQL_DATAWARE HOUSE\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			  Firstrow = 2,
			  Fieldterminator = ',',
			  tablock 
		);
		
		 set @end_time = GETDATE();
		 print'>> Load duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + 'seconds';
		 print '--------------------------';

		 
		 set @start_time = GETDATE();

		print'>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

		print'>> Inserting data into : Bronze.crm_prd_info';
		Bulk insert bronze.crm_prd_info
		FROM 'C:\Users\5425\Desktop\SQL_DATAWARE HOUSE\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',    
			ROWTERMINATOR = '\n',
			KEEPNULLS,                   
			TABLOCK
		);
		 set @end_time = GETDATE();
		 print'>> Load duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + 'seconds';
		 print '--------------------------';
		  

        
		 set @start_time = GETDATE();
	
		print'>> Truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;

		print'>> Inserting data into : Bronze.crm_sales_details';
		Bulk insert bronze.crm_sales_details
		from 'C:\Users\5425\Desktop\SQL_DATAWARE HOUSE\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			  Firstrow = 2,
			  Fieldterminator = ',',
			  tablock 
		);

		 set @end_time = GETDATE();
		 print'>> Load duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + 'seconds';
		 print '--------------------------';

		print'----------------------------------------------------';
		print'Loading ERP Tables';
		print'----------------------------------------------------';

	    
		 set @start_time = GETDATE();

		print'>> Truncating Table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;

		print'>> Inserting data into : Bronze.erp_loc_a101';
		Bulk insert bronze.erp_loc_a101
		from 'C:\Users\5425\Desktop\SQL_DATAWARE HOUSE\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
			  Firstrow = 2,
			  Fieldterminator = ',',
			  tablock 
		);

		 set @end_time = GETDATE();
		 print'>> Load duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + 'seconds';
		 print '--------------------------';
		
		 set @start_time = GETDATE();

		print'>> Truncating Table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;

		print'>> Inserting data into : Bronze.erp_cust_az12';
		Bulk insert bronze.erp_cust_az12
		from 'C:\Users\5425\Desktop\SQL_DATAWARE HOUSE\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
			  Firstrow = 2,
			  Fieldterminator = ',',
			  tablock 
		);
		 set @end_time = GETDATE();
		 print'>> Load duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + 'seconds';
		 print '--------------------------';
	    
		 set @start_time = GETDATE();
		print'>> Truncating Table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;


		print'>> Inserting data into : Bronze.erp_px_cat_g1v2';
		Bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\5425\Desktop\SQL_DATAWARE HOUSE\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			  Firstrow = 2,
			  Fieldterminator = ',',
			  tablock
			  );
			   set @end_time = GETDATE();
		      print'>> Load duration: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + 'seconds';
		       print '--------------------------';
			   
			   set @batch_end_time = GETDATE();
			   print'=======================================';
			   print'Loading bronze layer is completed';
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
