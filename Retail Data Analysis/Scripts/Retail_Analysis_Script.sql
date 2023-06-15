Create database Retails;

Use Retails;

USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE demographic_RAW
(AGE_DESC	CHAR(20),
MARITAL_STATUS_CODE	CHAR(5),
INCOME_DESC	VARCHAR(40),
HOMEOWNER_DESC	VARCHAR(40),
HH_COMP_DESC	VARCHAR(50),
HOUSEHOLD_SIZE_DESC	VARCHAR(50),
KID_CATEGORY_DESC	VARCHAR(40),
household_key INT PRIMARY KEY
);

CREATE OR REPLACE TABLE CAMPAIGN_DESC_RAW
(DESCRIPTION CHAR(10),	
CAMPAIGN	INT ,
START_DAY	INT,
END_DAY INT,
PRIMARY KEY (DESCRIPTION),
UNIQUE (CAMPAIGN));


CREATE OR REPLACE TABLE CAMPAIGN_RAW
(DESCRIPTION	CHAR(10) ,
household_key	INT,
CAMPAIGN INT,
FOREIGN KEY (DESCRIPTION) references CAMPAIGN_DESC_RAW(DESCRIPTION) ,
FOREIGN KEY (CAMPAIGN) references CAMPAIGN_DESC_RAW(CAMPAIGN),
FOREIGN KEY (household_key) references demographic_RAW(household_key)
);

CREATE OR REPLACE TABLE PRODUCT_RAW
(PRODUCT_ID	INT PRIMARY KEY,
MANUFACTURER 	INT,
DEPARTMENT	VARCHAR(50),
BRAND	VARCHAR(30),
COMMODITY_DESC	VARCHAR(65),
SUB_COMMODITY_DESC VARCHAR(65)	,
CURR_SIZE_OF_PRODUCT VARCHAR(15)
);


CREATE OR REPLACE TABLE COUPON_RAW
(COUPON_UPC	INT,
PRODUCT_ID	INT,
CAMPAIGN INT,
FOREIGN KEY (PRODUCT_ID) references PRODUCT_RAW(PRODUCT_ID),
FOREIGN KEY (CAMPAIGN) references CAMPAIGN_DESC_RAW(CAMPAIGN)
);


CREATE OR REPLACE TABLE COUPON_REDEMPT_RAW
(household_key	INT,
DAY	INT,
COUPON_UPC	INT,
CAMPAIGN INT,
FOREIGN KEY (household_key) references demographic_RAW(household_key),
FOREIGN KEY (CAMPAIGN) references CAMPAIGN_DESC_RAW(CAMPAIGN)
);

CREATE OR REPLACE TABLE TRANSACTION_RAW 
(household_key	INT,
BASKET_ID	INT,
DAY	INT,
PRODUCT_ID	INT,
QUANTITY	INT,
SALES_VALUE	FLOAT,
STORE_ID	INT,
RETAIL_DISC	FLOAT,
TRANS_TIME	INT,
WEEK_NO	INT,
COUPON_DISC	INT,
COUPON_MATCH_DISC INT,
FOREIGN KEY (PRODUCT_ID) references PRODUCT_RAW(PRODUCT_ID),
FOREIGN KEY (household_key) references demographic_RAW(household_key)
);

----------------------------------------------------AWS (S3) INTEGRATION------------------------------------------------------------

CREATE OR REPLACE STORAGE integration s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN ='arn:aws:iam::441615131678:role/retailrole' 
STORAGE_ALLOWED_LOCATIONS =('s3://retailraw/');

DESC integration s3_int;


CREATE OR REPLACE STAGE RETAIL
URL ='s3://retailraw'
file_format = CSV
storage_integration = s3_int;

LIST @RETAIL;

SHOW STAGES;

------------------------------------------------------------------------------------------------------------------------------------



CREATE OR REPLACE PIPE RETAIL_SNOWPIPE_DEMOGRAPHIC AUTO_INGEST = TRUE AS
COPY INTO "RETAILS"."PUBLIC"."DEMOGRAPHIC_RAW" --yourdatabase -- your schema ---your table
FROM '@RETAIL/DEMOGRAPHIC/' --s3 bucket subfolde4r name
FILE_FORMAT = CSV; --YOUR CSV FILE FORMAT NAME

CREATE OR REPLACE PIPE RETAIL_SNOWPIPE_CAMPAIGN_DESC AUTO_INGEST = TRUE AS
COPY INTO "RETAILS"."PUBLIC"."CAMPAIGN_DESC_RAW"
FROM '@RETAIL/CAMPAIGN_DESC/' 
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE RETAIL_SNOWPIPE_CAMPAIGN AUTO_INGEST = TRUE AS
COPY INTO "RETAILS"."PUBLIC"."CAMPAIGN_RAW"
FROM '@RETAIL/CAMPAIGN/' 
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE RETAIL_SNOWPIPE_PRODUCT AUTO_INGEST = TRUE AS
COPY INTO "RETAILS"."PUBLIC"."PRODUCT_RAW"
FROM '@RETAIL/PRODUCT/' 
FILE_FORMAT = CSV;


CREATE OR REPLACE PIPE RETAIL_SNOWPIPE_COUPON AUTO_INGEST = TRUE AS
COPY INTO "RETAILS"."PUBLIC"."COUPON_RAW"
FROM '@RETAIL/COUPON/' 
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE RETAIL_SNOWPIPE_COUPON_REDEMPT  AUTO_INGEST = TRUE AS
COPY INTO "RETAILS"."PUBLIC"."COUPON_REDEMPT_RAW"
FROM '@RETAIL/COUPON_REDEMPT/' 
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE RETAIL_SNOWPIPE_TRANSACTION  AUTO_INGEST = TRUE AS
COPY INTO "RETAILS"."PUBLIC"."TRANSACTION_RAW"
FROM '@RETAIL/TRANSACTION/' 
FILE_FORMAT = CSV;

SHOW PIPES;

SELECT COUNT(*) FROM demographic_RAW;
SELECT COUNT(*) FROM CAMPAIGN_DESC_RAW;
SELECT COUNT(*) FROM CAMPAIGN_RAW;
SELECT COUNT(*) FROM PRODUCT_RAW;
SELECT COUNT(*) FROM COUPON_RAW;
SELECT COUNT(*) FROM COUPON_REDEMPT_RAW;
SELECT COUNT(*) FROM TRANSACTION_RAW;

----------------------------------------------------------PIPEREFRESH---------------------------------------------------------------

ALTER PIPE RETAIL_SNOWPIPE_DEMOGRAPHIC refresh;
ALTER PIPE  RETAIL_SNOWPIPE_CAMPAIGN_DESC refresh;
ALTER PIPE  RETAIL_SNOWPIPE_CAMPAIGN refresh;
ALTER PIPE  RETAIL_SNOWPIPE_PRODUCT refresh;
ALTER PIPE  RETAIL_SNOWPIPE_COUPON refresh;
ALTER PIPE  RETAIL_SNOWPIPE_COUPON_REDEMPT refresh;
ALTER PIPE  RETAIL_SNOWPIPE_TRANSACTION refresh;

--------------------------------------------------CheckingTables--------------------------------------------------------------------

SELECT * FROM demographic_RAW;
SELECT * FROM CAMPAIGN_DESC_RAW;
SELECT * FROM CAMPAIGN_RAW;
SELECT * FROM PRODUCT_RAW;
SELECT * FROM COUPON_RAW;
SELECT * FROM COUPON_REDEMPT_RAW;
SELECT * FROM TRANSACTION_RAW;

SELECT * FROM CAMPAIGN_DESC_NEW;
SELECT * FROM COUPON_REDEMPT_NEW;
SELECT * FROM TRANSACTION_NEW;
------------------------------------------------------------------------------------------------------------------------------------

SELECT DISTINCT(DEPARTMENT),COUNT(*) AS TOTAL_PRODUCT 
FROM PRODUCT_RAW
GROUP BY 1
ORDER BY 2 DESC;
---------------------------------------------------------------Cleaning-------------------------------------------------------------
select * from product_raw WHERE LENGTH(TRIM(COMMODITY_DESC)) = 0;

delete from product_raw WHERE LENGTH(TRIM(COMMODITY_DESC)) = 0;

select * from Coupo WHERE LENGTH(TRIM(COMMODITY_DESC)) = 0;

---------------------------------------------KPIs (Not Mandatory in SQL[Snowflake])-------------------------------------------------

/*Customer Demographics KPIs:
o Count of unique households: Measure the total number of unique households in 
the Demographic table.
o Household composition distribution: Analyze the distribution of household 
compositions (HH_COMP_DESC) to understand the composition of households.
o Age distribution: Calculate the percentage or count of customers in different age 
groups (AGE_DESC).
o Marital status distribution: Analyze the proportion of customers in different 
marital status categories (MARITAL_STATUS_CODE).
o Income distribution: Determine the distribution of customers across income levels 
(INCOME_DESC).
o Homeownership distribution: Calculate the percentage or count of customers who 
own or rent their homes (HOMEOWNER_DESC).*/

select * from demographic_raw;

select count(HOUSEHOLD_KEY) from demographic_raw; --2500 #same result

select count(distinct HOUSEHOLD_KEY) as Total_Households from demographic_raw -- 2,500 #same result

select HH_COMP_DESC, count(distinct HOUSEHOLD_KEY) as Total_Households
from demographic_raw
group by 1
order by 2 desc;

select AGE_DESC, Total_Households, round(total_Households/2500 * 100, 2) AS Perc_Agewise_Household
FROM
(select AGE_DESC, count(distinct HOUSEHOLD_KEY) as Total_Households
from demographic_raw
group by 1
order by 2 desc)
group by 1,2;

select MARITAL_STATUS_CODE, Total_Households, round(total_Households/2500 * 100, 2) AS Perc_Agewise_Household
FROM
(select MARITAL_STATUS_CODE, count(distinct HOUSEHOLD_KEY) as Total_Households
from demographic_raw
group by 1
order by 2 desc)
group by 1,2;

select INCOME_DESC, Total_Households, round(total_Households/2500 * 100, 2) AS Perc_Agewise_Household
FROM
(select INCOME_DESC, count(distinct HOUSEHOLD_KEY) as Total_Households
from demographic_raw
group by 1
order by 2 desc)
group by 1,2;

select HOMEOWNER_DESC, Total_Households, round(total_Households/2500 * 100, 2) AS Perc_Agewise_Household
FROM
(select HOMEOWNER_DESC, count(distinct HOUSEHOLD_KEY) as Total_Households
from demographic_raw
group by 1
order by 2 desc)
group by 1,2;

select t.household_key,d.age_desc,d.marital_status_code,d.income_desc,d.homeowner_desc,
avg(t.sales_value)as AVG_AMOUNT,avg(t.retail_disc) as AVG_RETAIL_DIS, avg(t.coupon_disc) as AVG_COUPON_DISC,avg(t.coupon_match_disc) as AVG_COUP_MATCH_DISC
FROM transaction_new T left outer join demographic_raw D on T.HOUSEHOLD_KEY = D.HOUSEHOLD_KEY
group by 1,2,3,4,5
order by 1;

CREATE OR REPLACE PROCEDURE Demographics_kpi()
RETURNS STRING
LANGUAGE SQL
AS
$$
CREATE OR REPLACE TABLE Demographics_kpi AS (select t.household_key,d.age_desc,d.marital_status_code,d.income_desc,d.homeowner_desc,
avg(t.sales_value)as AVG_AMOUNT,avg(t.retail_disc) as AVG_RETAIL_DIS, avg(t.coupon_disc) as AVG_COUPON_DISC,avg(t.coupon_match_disc) as AVG_COUP_MATCH_DISC
FROM transaction_new T left outer join demographic_raw D on T.HOUSEHOLD_KEY = D.HOUSEHOLD_KEY
group by 1,2,3,4,5
order by 1);
$$;

SHOW PROCEDURES;

call Demographics_kpi();

create or replace task Demographics_kpi_task
warehouse = COMPUTE_WH
SCHEDULE = '5 minute'  --##schedule time was 8 30 so i keep buffer time 15 min
as call Demographics_kpi()

SHOW TASKS;

ALTER TASK   Demographics_kpi_TASK RESUME;
ALTER TASK  Demographics_kpi_TASK SUSPEND; 

select * from Demographics_kpi;


/* Campaign KPIs:
o Number of campaigns: Count the total number of campaigns in the Campaign 
table.
o Campaign duration: Calculate the duration of each campaign by subtracting the 
start day from the end day (in the Campaign_desc table).
o Campaign effectiveness: Analyze the number of households associated with each 
campaign (in the Campaign table) to measure campaign reach */

select * from CAMPAIGN_RAW;
SELECT * FROM CAMPAIGN_DESC_NEW;

select count(CAMPAIGN) from CAMPAIGN_DESC_NEW; -- Total number of campaign 30

-- Already done subtraction (start day from the end day) in python and create a column 
select CAMPAIGN,sum(campaign_duration) AS Total_Duration from CAMPAIGN_DESC_NEW 
group by 1
order by 1;

select campaign, count(household_key) AS Total_Household_Particated from CAMPAIGN_RAW
group by 1
order by 2 desc;

select CD.CAMPAIGN, mode(D.AGE_DESC) AS Most_Ptcipt_AgeGroup, SUM(CD.CAMPAIGN_DURATION) AS CAMPAIGN_DURATION, COUNT(C.HOUSEHOLD_KEY) AS HOUSEHOLD_Pticipt, SUM(CR.COUPON_UPC) AS Total_Coupon_UPC, AVG(CR.COUPON_UPC) AS Average_Coupon_UPC
from CAMPAIGN_DESC_NEW CD
LEFT OUTER JOIN CAMPAIGN_RAW C ON CD.CAMPAIGN = C.CAMPAIGN
LEFT OUTER JOIN DEMOGRAPHIC_RAW D ON C.HOUSEHOLD_KEY = D.HOUSEHOLD_KEY
LEFT OUTER JOIN COUPON_REDEMPT_NEW CR ON CR.CAMPAIGN = CD.CAMPAIGN
group by 1
order by 1;

CREATE OR REPLACE PROCEDURE Campaign_kpi()
RETURNS STRING
LANGUAGE SQL
AS
$$
CREATE OR REPLACE TABLE Campaign_kpi AS (select CD.CAMPAIGN, mode(D.AGE_DESC) AS Most_Ptcipt_AgeGroup, SUM(CD.CAMPAIGN_DURATION) AS CAMPAIGN_DURATION, COUNT(C.HOUSEHOLD_KEY) AS HOUSEHOLD_Pticipt, SUM(CR.COUPON_UPC) AS Total_Coupon_UPC, AVG(CR.COUPON_UPC) AS Average_Coupon_UPC
from CAMPAIGN_DESC_NEW CD
LEFT OUTER JOIN CAMPAIGN_RAW C ON CD.CAMPAIGN = C.CAMPAIGN
LEFT OUTER JOIN DEMOGRAPHIC_RAW D ON C.HOUSEHOLD_KEY = D.HOUSEHOLD_KEY
LEFT OUTER JOIN COUPON_REDEMPT_NEW CR ON CR.CAMPAIGN = CD.CAMPAIGN
group by 1
order by 1);
$$;

call Campaign_kpi();

create or replace task Campaign_kpi_task
warehouse = COMPUTE_WH
SCHEDULE = '5 minute'  --##schedule time was 8 30 so i keep buffer time 15 min
as call Campaign_kpi()

SHOW TASKS;

ALTER TASK   Campaign_kpi_TASK RESUME;
ALTER TASK  Campaign_kpi_TASK SUSPEND; 

select * from Campaign_kpi;

/* Coupon KPIs:
o Coupon redemption rate: Calculate the percentage of coupons redeemed (from the 
coupon_redempt table) compared to the total number of coupons distributed (from 
the Coupon table).
o Coupon usage by campaign: Measure the number of coupon redemptions (from 
the coupon_redempt table) for each campaign (in the Coupon table). */

select * from coupon_raw;

select * from coupon_redempt_new;

select count(DISTINCT COUPON_UPC) from coupon_raw;
select count(COUPON_UPC) from coupon_raw;

select count(DISTINCT COUPON_UPC) from coupon_redempt_new;
select count(COUPON_UPC) from coupon_redempt_new;

select Round(count(distinct CR.COUPON_UPC)/count(distinct C.COUPON_UPC)*100,2)
from COUPON_RAW C LEFT OUTER JOIN COUPON_REDEMPT_NEW CR ON C.COUPON_UPC = CR.COUPON_UPC;

select C.CAMPAIGN ,count(DISTINCT cr.coupon_upc)
from COUPON_RAW C LEFT OUTER JOIN COUPON_REDEMPT_NEW CR ON C.COUPON_UPC = CR.COUPON_UPC
group by 1
order by 1;

select MARITAL_STATUS_CODE, Total_Households, round(total_Households/2500 * 100, 2) AS Perc_Agewise_Household
FROM
(select MARITAL_STATUS_CODE, count(distinct HOUSEHOLD_KEY) as Total_Households
from demographic_raw
group by 1
order by 2 desc)
group by 1,2;

/* Product KPIs:
o Sales value: Calculate the total sales value for each product (in the 
Transaction_data table) to identify top-selling products.
o Manufacturer distribution: Analyze the distribution of products across different 
manufacturers (in the Product table).
o Department-wise sales: Measure the sales value by department (in the Product 
table) to understand which departments contribute most to revenue.
o Brand-wise sales: Calculate the sales value for each brand (in the Product table) to 
identify top-selling brands. */

select * from product_raw;
select * from TRANSACTION_NEW;

select P.COMMODITY_DESC , sum(T.SALES_VALUE)
FROM PRODUCT_RAW P LEFT OUTER JOIN TRANSACTION_NEW T ON P.PRODUCT_ID = T.PRODUCT_ID
group by 1
order by 2 desc;

SELECT COMMODITY_DESC , COUNT(MANUFACTURER) FROM PRODUCT_RAW --EACH PRODUCT WITH DIFFERENT NUMBER OF MANUFACTURES
GROUP BY 1
ORDER BY 2 DESC;

select P.DEPARTMENT , sum(T.SALES_VALUE)
FROM PRODUCT_RAW P LEFT OUTER JOIN TRANSACTION_NEW T ON P.PRODUCT_ID = T.PRODUCT_ID
group by 1
order by 2 desc;

select P.BRAND , sum(T.SALES_VALUE)
FROM PRODUCT_RAW P LEFT OUTER JOIN TRANSACTION_NEW T ON P.PRODUCT_ID = T.PRODUCT_ID
group by 1
order by 2 desc;


SELECT P.BRAND, P.DEPARTMENT, P.COMMODITY_DESC, SUM(T.SALES_VALUE), AVG(T.SALES_VALUE), sum(T.QUANTITY), count(C.COUPON_UPC) 
FROM PRODUCT_RAW P
LEFT OUTER JOIN TRANSACTION_NEW T ON P.PRODUCT_ID = T.PRODUCT_ID
LEFT OUTER JOIN COUPON_RAW C ON P.PRODUCT_ID = C.PRODUCT_ID
GROUP BY 1,2,3
ORDER BY 4 DESC;


CREATE OR REPLACE PROCEDURE Product_kpi()
RETURNS STRING
LANGUAGE SQL
AS
$$
CREATE OR REPLACE TABLE Product_kpi AS (SELECT P.BRAND, P.DEPARTMENT, P.COMMODITY_DESC, SUM(T.SALES_VALUE) AS TOTAL_SALES, AVG(T.SALES_VALUE) AS AVERAGE_SALES, SUM(T.QUANTITY) AS QUATITY_SOLD, count(C.COUPON_UPC)  AS COUPONS 
FROM PRODUCT_RAW P
LEFT OUTER JOIN TRANSACTION_NEW T ON P.PRODUCT_ID = T.PRODUCT_ID
LEFT OUTER JOIN COUPON_RAW C ON P.PRODUCT_ID = C.PRODUCT_ID
GROUP BY 1,2,3
ORDER BY 4 DESC);
$$;

call Product_kpi();

create or replace task Product_kpi_task
warehouse = COMPUTE_WH
SCHEDULE = '5 minute'  --##schedule time was 8 30 so i keep buffer time 15 min
as call Product_kpi()

SHOW TASKS;

ALTER TASK   Product_kpi_TASK RESUME;
ALTER TASK  Product_kpi_TASK SUSPEND; 

select * from Product_kpi;

/* Transaction KPIs:
o Total sales value: Calculate the sum of sales values (in the Transaction_data table) 
to measure overall revenue.
o Average transaction value: Calculate the average sales value per transaction to 
understand customer spending patterns.
o Quantity sold: Measure the total quantity sold (in the Transaction_data table) to 
understand product demand.
o Discounts: Analyze the amount and impact of discounts (RETAIL_DISC, 
COUPON_DISC, COUPON_MATCH_DISC) on sales value.
*/

select * from TRANSACTION_NEW;


select sum(SALES_VALUE) FROM TRANSACTION_NEW; -- 8,057,453.08


select AVG(SALES_VALUE) FROM TRANSACTION_NEW; -- 3.104119794

SELECT (SUM(SALES_VALUE)/count(SALES_VALUE)) AS AVG_SALES_PER_TRNX FROM TRANSACTION_NEW; --3.104119794


SELECT SUM(QUANTITY) FROM TRANSACTION_NEW -- Total Quantity SOLD 260685622 

select P.COMMODITY_DESC , SUM(T.QUANTITY) -- Quantity sold on the basis of product
FROM PRODUCT_RAW P LEFT OUTER JOIN TRANSACTION_NEW T ON P.PRODUCT_ID = T.PRODUCT_ID
group by 1
order by 2 desc;


select * from transaction_new where COUPON_DISC < 0;
select * from transaction_new where COUPON_MATCH_DISC < 0;

SELECT sum(RETAIL_DISC) FROM TRANSACTION_NEW
SELECT sum(COUPON_DISC) FROM TRANSACTION_NEW
SELECT sum(COUPON_MATCH_DISC) FROM TRANSACTION_NEW
 
SELECT SUM(SALES_VALUE) , (SUM(SALES_VALUE) + (SUM(RETAIL_DISC) + sum(COUPON_DISC) + sum(COUPON_MATCH_DISC))) AS SALES_AFTER_DISCOUNT FROM TRANSACTION_NEW;

--Note :- I made only those KPI table which I was necessary in my opinon
