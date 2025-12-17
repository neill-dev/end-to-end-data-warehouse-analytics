
--Change-over-time in our data(Analysze sales performance over time)
select
month(order_Date) as order_month,
year(order_date) as order_year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers
from gold.fact_sales
where order_date is not null
group by year(order_date) ,MONTH(order_date)
order by year(order_date) ,month(order_date)


--Cumulative Analysis
--Aggregate the data progressively over time
--Helps to understand whether our business is growing or declining.

--Calucalte the total sales per month
-- and the running total sales over time

select 
order_date,
total_sales,
--window function
sum(total_sales) over (partition by order_Date order by order_date) as running_total_sales,
avg(avg_price) over (order by order_date) as moving_average_price
from
(
select 
datetrunc(month,order_Date) as order_Date,
sum(sales_amount) as total_sales,
avg(price) as avg_price
from gold.fact_sales
where order_Date is not null
group by DATETRUNC(month,order_date)
)t

--Performance Analysis (Current[measure] -Target[Measure]
/* Analyze the yearly performance of products by comaparing thier sales
to both the average sales performance of the product and the previous year's sales*/

with yearly_product_sales as(
select
year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where f.order_date is not null
group by 
year(f.order_date),
p.product_name)

select
order_year,
product_name,
current_Sales,
avg(current_sales) over (partition by product_name) as avg_sales,
current_sales - Avg(current_sales) over (partition by product_name) as diff_avg,
case when current_sales -  Avg(current_sales) over (partition by product_name) > 0 then 'Above Avg'
      when current_sales -  Avg(current_sales) over (partition by product_name) < 0 Then 'Below avg'
	  else 'Avg'
End avg_change,
lag(current_sales) over (partition by product_name order by order_year) as py_year_sales,
current_sales - lag(current_sales) over (partition by product_name order by order_year) as diff_year,
case when current_sales - lag(current_sales) over (partition by product_name order by order_year) > 0 Then 'Increase '
      when current_sales - lag(current_sales) over (partition by product_name order by order_year) < 0 Then 'Decrease'
	  else 'No Change'
end py_change
from yearly_product_sales
order by product_name , order_year

-- Yearly Performance of the product
select
year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where f.order_date is not null
group by 
year(f.order_date),
p.product_name


--PART TO WHOLE ANALYSIS( Analyze how an individual part is performing compared to the overall,
-- allowing us to understand which category has the greatest impact on the business.


-- Which categoreis contibutes the most to overall sales.

with category_sales as (
select 
category,
sum(sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by category
)

select 
category,
total_sales,
sum(total_sales) over() overall_sales,
concat(round((cast(total_sales as float)  / sum(total_Sales) over()) * 100,2),'%' )as percentile_of_total
from category_Sales
order by total_sales desc  

--DATA SEGMENTAION (group the data based on specific range.
-- helps understand the correlation between two measures.

--Segment products into cost ranges and
-- count how many products fall into each segment
-- if our dimensions are not enough to create insights we can one of our measures converted into dimensions
--using (case when) and aggregate other measures using these dimension 
with product_segments as (

select 
product_key,
product_name,
cost,
case when cost < 100 then 'Below 100'
     when cost between 100 and 500 then '100-500'
	 when cost between 500 and 1000 then '500-1000'
	 else 'above 1000'
end cost_Range
	 from gold.dim_products )

	 select 
	 cost_range,
	 count(product_key) as total_products
	 from product_segments
	 group by cost_Range
	 order  by total_products desc



--Group customers into three segments based on their spending behavior.
--VIP: at least 12 months of history and spending more than $5000.
--Regular: at least 2 months of history but spending $5000 or less.
--New: lifespan less than 12 months.

-- And find the total number of customers by each group
--In these query we did 2 times using of dimension for making a measures 
with cusotmer_spending as (
	select
	c.customer_key,
	sum(f.sales_amount) as total_spending,
	min(order_Date) as first_order,
	max(order_date) as last_order,
	datediff(month,min(order_Date), max(order_date)) as lifespan
	from gold.fact_sales f
	left join gold.dim_customers c
	on f.customer_key = c.customer_key
	group by c.customer_key )

select 
customer_segment,
count(customer_key) as total_customers
from(
	select 
	customer_key,
	case when lifespan >= 12 and total_spending >= '5000' then 'VIP'
		 when lifespan >= 12 and  total_spending <= '5000' Then 'Regular'
		 else 'New'
	end customer_segment
	from cusotmer_spending )t

group by customer_segment
order by total_customers desc
