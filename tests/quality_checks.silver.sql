
-- I WROTE THE COMMENTS TO UNDERSTAND EACH AND EVERY LOGIC 

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

--EVEN I WROTE THE COMMENTS TO UNDERSTAND EACH AND EVERY LOGIC 

/*========================================================================================================
Check silver.crm_cust_info 
===========================================================================================================
*/
--Check for Unwanted Spaces
--Expectation : No results
select cst_firstname           -- use silver in every query where is having bronze to check duplications
from bronze.crm_cust_info
Where cst_firstname != TRIM (cst_firstname) /*1. These logic checks if cst_firstname or lastname or gndr(ALL THE STRINGS VALUES)
                                   having any spaces  in front side or back side these this logic only checks the spaces if have 
								   spaces it will give the ouput if not it dont give any output like
								  in case of gendr there is no trailing or leading spaces so it was totally empty*/

                                 
 --4. Data Standardization and Consistency (quality check 
select distinct cst_material_status    
from bronze.crm_cust_info
--5
select 
cst_id,
COUNT(*)
from Silver.crm_cust_info
group by cst_id
having COUNT(*) > 1  

/*=========================================================================================================================  
   Check for silver.crm_prd_info 
===========================================================================================================================
*/
--check for Nulls or Duplicates in primary key
--Expectations : No Result

select 
prd_id,                                       --AFTER INSERTING THE CLEANED DATA INTO THE silver layer
COUNT(*)                                      -- from bronze layer now you can check data is cleaned or not
from bronze.crm_prd_info  -- by changing bronze.crm to siver . crm in every query written in these page.
group by prd_id
having COUNT(*) > 1 or prd_id is null

--check for unwanted spaces 
--expectations : No results

select prd_nm
from silver.crm_prd_info
where prd_nm != TRIM(prd_nm)

-- Check for nulls or negative numbers
-- Expectations : no results
select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null

-- Data Standadrdization & consistency
select Distinct prd_line
from silver.crm_prd_info

--Check For valid invalid date orders
select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt   -- 1.The start date should should be after the end date 
                                         -- start date smaller than end 
										               -- eg:-2009 start date so end date will be 2011
/*==================================================================================================================
 CHECK SILVER.CRM_SALES_DETAILS
==================================================================================================================
*/
-- AFTER INSERTION THE CLEANED GO FOR QUALITY CHECK WHEREEVER THE BRONZE IS THERE EDIT IT IN SILVER
-- AS WE INSERTD CLEANED DATA INTO SILVER AND CHECK IF ALL CONDIDITONS ARE GIVING EMPTY RESULTS == ALL GOOD
 SELECT * FROM silver.crm_sales_details
--Check for invalid dates ORDERS  ---( 1.it means that order date is never be negative so we have to check once 
select                            -- and it should be not a zero but here we have lot of zeros we have 
NULLIF(sls_order_dt,0)  sls_order_dt                    -- to solve this problem by using NULLIF BY MAKING 'NULL' if                                             
from bronze.crm_sales_details                             -- values are zero in sls_order_dt
 where  sls_order_dt <= 0
 or len(sls_order_dt) != 8  
 or sls_order_dt > 20500101 or sls_order_dt < 19000101
 or sls_order_dt < 19000101                              --2. (2010|12|29) here you can see the sls_order_dt should
                                                           -- be of 8 didgits only
-- Shippin dates modifying same process as order date
select                            
NULLIF(sls_ship_dt,0)  sls_ship_dt                                  
from bronze.crm_sales_details  
 where  sls_ship_dt <= 0
 or len(sls_ship_dt) != 8  
 or sls_ship_dt > 20500101 or sls_ship_dt < 19000101
 or sls_ship_dt < 19000101  

 -- Check for Due dates if invalid or not
 select                            
NULLIF(sls_due_dt,0)  sls_due_dt                                  
from bronze.crm_sales_details  
 where  sls_due_dt <= 0
 or len(sls_due_dt) != 8  
 or sls_due_dt > 20500101 or sls_due_dt < 19000101
 or sls_due_dt < 19000101 


 -- checking for invalid orders (2. Means order date must have be earlier than the shipping date or due date)
 select                           -- after execution we got empy rows means data is already cleaned and order
 *                               -- order date is not greater than ship date and also order should not 
 from bronze.crm_sales_details    -- greater than due date.
 where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

 --4.CHECK DATE CONSISTENCY : Between Sales, Quantity , and Price 
-- >> Sales = Quantity * Price
-- >> Values must not be null , zero , or negtive .

select  distinct                        --RULES- 1. IF (SALES) IS NEGATIVE , ZERO , OR NULL , DERIVE IT USING
sls_sales as olds_sls_sales,                                      -- QUANTITY AND PRICE 
sls_quantity,                                  --2. IF (PRICE) IS ZERO OR NULL , CALCULATE IT USING SALES AND 
sls_price   as old_sls_price,                                          -- QUANTITY
                                                          --3. IF (PRICE) IS NEGATIVE , CONVERT IT INTO A POSITIVE VALUE
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
     Then sls_quantity * abs(sls_price) 
	 else sls_sales
end as sls_sales,

case when sls_price is null or sls_price < = 0             --2. if sls_quantity columns got nulls
then sls_sales / nullif(sls_quantity,0)                     -- replace it with zero so calculation will be smoothen.               
else sls_price 
End as sls_price
from bronze.crm_sales_details                        
                                                  
where sls_sales != sls_quantity * sls_price                     -- 1.checking(sales = quantity × price)
or sls_sales is null or sls_quantity is null or sls_price is null  -- checking nulls
or sls_sales <= 0 or sls_quantity <= 0  or sls_price <= 0           --- checking zeros
order by  sls_sales,                                              --ORDER BY sls_sales First, rows are sorted by sls_sales                                                                        
sls_quantity,
sls_price 


-- 5.check for table joining 
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info) -- in these logic we checked that
                                                                    -- sls_prd_key is matching with silver.crm_prd_info
																	-- because at the time of joining both tables
																	-- if any not matched so problem occur,
																	-- but we checked now we got anser zero means every key is 
																	--matching.
select
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info) -- here we connected cust_info table with
  
 
 -- Here whatever the data types i changed in sales and due and price( integer to date) datatype at last
  -- at last we have to create a new columns data so our cleaned date is going to fit in those table 
  -- which is created already.

  
/*===================================================================================================================
  CHECK SILVER.ERP_CUST_AZ12
  ==================================================================================================================*/
 
  --CHECK DATA QUALITY BY WRITING SILVER. IN PLACE OF BRONZE 

select
cid,
case when cid like 'NAS%' Then substring (cid,4,len(cid)) -- 1.Here we informed that cid starting with NAS
else cid                                                   -- then cut down the remaining part from 4th charachter 
end cid,                                                      -- dyanamically(so used the length function).
bdate,
gen
from bronze.erp_cust_az12
where case when cid like 'NAS%' Then substring (cid,4,len(cid)) 
else cid                                                         --2.Here we joined the tables for matching 
end  not in ( select distinct cst_key from silver.crm_cust_info)  -- if ids matched the table printed empty
                                                                 -- because we said (not in) key word

-- Birth date checking correct or not
select 
bdate
from bronze.erp_cust_az12                       -- in this logic we used 1924 because to check the person 
where bdate < '1924-01-01' or bdate > getdate()  -- is of 100 years above or not  because erp will give 
                                                 -- the information from start to current date (2024) so 1924 below date got means peoples crossed 100 years
												 -- bdate > getdate() its impossible two scenarios checked

-- Data Standardization & consistency
select distinct 
gen,
case when upper(trim(gen)) in ('F', 'Female' ) Then 'Female'
     when upper(trim(gen)) in ('M', 'Male') Then 'Male'
	 else'n/a'
end as gen
from silver.erp_cust_az12
/*===================================================================================================================
CHECK SILVER.ERP_LOC_A101
=====================================================================================================================
*/
--Check after insertion bronze to silver
-- data standardization & consistency
select distinct cntry
from bronze.erp_loc_a101


/*=========================================================================================================================
CHECK SILVER.ERP_PX_CAT_G1V2
==========================================================================================================================
*/--Unwanted spaces
select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)

-- Data Standardization & consistency
select distinct
maintenance
from bronze.erp_px_cat_g1v2
