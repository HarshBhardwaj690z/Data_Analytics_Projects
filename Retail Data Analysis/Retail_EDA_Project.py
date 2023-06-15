#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install snowflake-connector-python


# In[2]:


pip install snowflake-sqlalchemy


# In[6]:


pip install "snowflake-connector-python[pandas]"


# In[1]:


import numpy as np
import pandas as pd
import pandas_profiling
import matplotlib.pyplot as plt
import getpass
import seaborn as sns
import snowflake.connector


# In[2]:


conn = snowflake.connector.connect(
       user = 'VHARSHBHARDWAJ',
       password = getpass.getpass('Your Snowflake Password: '),
       ##password='((whatever your password is))',
       ##  account = https://uyvaacf-ic87213.snowflakecomputing.com/
       account = 'uyvaacf-ic87213',
       database='RETAILS',
       schema='PUBLIC',
       warehouse='COMPUTE_WH',
 ) 


# In[3]:


cur = conn.cursor()


# In[4]:


select_demographic_RAW = 'SELECT * FROM demographic_RAW'
select_CAMPAIGN_DESC_RAW = 'SELECT * FROM CAMPAIGN_DESC_RAW'
select_CAMPAIGN_RAW = 'SELECT * FROM CAMPAIGN_RAW'
select_PRODUCT_RAW = 'SELECT * FROM PRODUCT_RAW'
select_COUPON_RAW = 'SELECT * FROM COUPON_RAW'
select_COUPON_REDEMPT_RAW = 'SELECT * FROM COUPON_REDEMPT_RAW'
select_TRANSACTION_RAW = 'SELECT * FROM TRANSACTION_RAW'


# In[5]:


cur.execute(select_demographic_RAW)
demographic_RAW = cur.fetch_pandas_all()


# In[6]:


cur.execute(select_CAMPAIGN_DESC_RAW)
CAMPAIGN_DESC_RAW = cur.fetch_pandas_all()


# In[7]:


cur.execute(select_CAMPAIGN_RAW)
CAMPAIGN_RAW = cur.fetch_pandas_all()


# In[8]:


cur.execute(select_PRODUCT_RAW)
PRODUCT_RAW = cur.fetch_pandas_all()


# In[9]:


cur.execute(select_COUPON_RAW)
COUPON_RAW = cur.fetch_pandas_all()


# In[10]:


cur.execute(select_COUPON_REDEMPT_RAW)
COUPON_REDEMPT_RAW = cur.fetch_pandas_all()


# In[11]:


cur.execute(select_TRANSACTION_RAW)
TRANSACTION_RAW = cur.fetch_pandas_all()


# In[12]:


cur.close()
conn.close()


# # UNDERSTAND DATASET

# In[13]:


demographic_RAW.head(5)


# In[14]:


demographic_RAW['AGE_DESC'].value_counts()


# In[15]:


demographic_RAW['HOUSEHOLD_SIZE_DESC'].value_counts()


# In[16]:


CAMPAIGN_DESC_RAW.head(5)


# In[17]:


CAMPAIGN_RAW.head(5)


# In[18]:


PRODUCT_RAW.head(5)


# In[19]:


COUPON_RAW.head(5)


# In[20]:


COUPON_REDEMPT_RAW.head(5)


# In[21]:


TRANSACTION_RAW.head(5)


# In[22]:


TRANSACTION_RAW.dtypes


# In[23]:


CAMPAIGN_DESC_RAW.shape


# In[24]:


COUPON_REDEMPT_RAW.shape


# In[25]:


COUPON_RAW.shape


# In[26]:


demographic_RAW.shape


# In[27]:


PRODUCT_RAW.shape


# In[28]:


TRANSACTION_RAW.shape


# In[29]:


CAMPAIGN_DESC_RAW.describe()


# In[30]:


PRODUCT_RAW.describe()


# In[31]:


demographic_RAW.describe()


# In[32]:


demographic_RAW.describe()


# In[33]:


COUPON_REDEMPT_RAW.describe()


# In[34]:


TRANSACTION_RAW.describe()


# In[35]:


CAMPAIGN_DESC_RAW.isnull().sum()


# In[36]:


PRODUCT_RAW.isnull().sum()


# In[37]:


demographic_RAW.isnull().sum()


# In[38]:


COUPON_RAW.isnull().sum()


# In[39]:


COUPON_REDEMPT_RAW.isnull().sum()


# In[40]:


TRANSACTION_RAW.isnull().sum()


# # DATA MODIFICATION

# In[41]:


from datetime import datetime,timedelta


# In[42]:


start_date = pd.to_datetime('2019-12-31')


# In[43]:


start_date


# In[44]:


TRANSACTION_RAW.head(20)


# In[45]:


TRANSACTION_RAW['DATE'] = start_date + pd.to_timedelta(TRANSACTION_RAW['DAY'],unit = 'D')


# In[46]:


TRANSACTION_RAW['DATE']


# In[47]:


CAMPAIGN_DESC_RAW.head(10)


# In[48]:


CAMPAIGN_DESC_RAW['Start_date']= start_date + pd.to_timedelta(CAMPAIGN_DESC_RAW['START_DAY'],unit='D')


# In[49]:


CAMPAIGN_DESC_RAW['End_date']=start_date + pd.to_timedelta(CAMPAIGN_DESC_RAW['END_DAY'],unit='D')


# In[50]:


CAMPAIGN_DESC_RAW.head(10)


# In[51]:


CAMPAIGN_DESC_RAW['Campaign_Duration'] = CAMPAIGN_DESC_RAW['END_DAY'] - CAMPAIGN_DESC_RAW['START_DAY']


# In[52]:


CAMPAIGN_DESC_RAW.head(20)


# In[53]:


COUPON_REDEMPT_RAW.head(10)


# In[54]:


COUPON_REDEMPT_RAW['Date'] = start_date + pd.to_timedelta(COUPON_REDEMPT_RAW['DAY'],unit='D')


# In[55]:


COUPON_REDEMPT_RAW.head(10)


# In[56]:


TRANSACTION_RAW['DATE'].max()


# In[57]:


CAMPAIGN_DESC_RAW['End_date'].max()


# In[58]:


COUPON_REDEMPT_RAW['Date'].max()


# he Average Campaign Duration is 46.6 days

# In[59]:


plt.figure(figsize=(15,5))
sns.barplot(x='CAMPAIGN',y='Campaign_Duration',data = CAMPAIGN_DESC_RAW)


# Campaign 15 Lasted more than 160 days

# In[60]:


CAMPAIGN_DESC_RAW.groupby('DESCRIPTION').aggregate({'CAMPAIGN':'count','Campaign_Duration':'mean'})


# There have been 19 type B campaigns, whose average length was 38 days. In comparison, there has been 6 type C campaigns of 75 days on average.

# In[61]:


CAMPAIGN_DESC_RAW['Start_month'] = CAMPAIGN_DESC_RAW['Start_date'].dt.strftime('%m')


# In[62]:


CAMPAIGN_DESC_RAW['End_month'] = CAMPAIGN_DESC_RAW['End_date'].dt.strftime('%m')


# In[63]:


CAMPAIGN_DESC_RAW['Start_year'] = CAMPAIGN_DESC_RAW['Start_date'].dt.strftime('%Y')


# In[64]:


CAMPAIGN_DESC_RAW['End_year'] = CAMPAIGN_DESC_RAW['End_date'].dt.strftime('%Y')


# In[65]:


CAMPAIGN_DESC_RAW.head(5)


# In[66]:


CAMPAIGN_RAW.head()


# In[67]:


# Checking for Unique Values
CAMPAIGN_RAW['HOUSEHOLD_KEY'].nunique()


# **There are 1584 households have participed to the campaign,[Total 2500 -(minus) Participated (1584)] leaving 916 households who never participated to any campaign.**

# In[68]:


CAMPAIGN_RAW.groupby('HOUSEHOLD_KEY')['CAMPAIGN'].count()


# In[69]:


plt.figure(figsize=(15,5))
CAMPAIGN_RAW.groupby('CAMPAIGN')['HOUSEHOLD_KEY'].count().plot.bar()
plt.ylabel('Number of Households Reached To')


# In Campaing 18 maximum number of households are participated.

# In[70]:


Coupon_Given = COUPON_RAW.groupby('CAMPAIGN').aggregate(Total_Product = ('PRODUCT_ID','nunique'), Total_Coupon_Given = ('COUPON_UPC','nunique'))


# In[71]:


Coupon_Given


# In[72]:


Coupon_Given.sort_values(by='Total_Product',ascending=False).head(10)


# Top 3 Is 13 18 and 8

# In[73]:


Coupon_Given = Coupon_Given.merge(right = CAMPAIGN_DESC_RAW, on = 'CAMPAIGN', how = 'left')


# In[74]:


Coupon_Given.head(10)


# In[75]:


Coupon_Given.columns #Checking which column we n eed and which we do not need


# In[76]:


Coupon_Given.loc[:,('CAMPAIGN','Total_Product','Total_Coupon_Given','Start_year','End_year','Start_month','End_month','DESCRIPTION','Campaign_Duration')].sort_values(by='Total_Product',
                    ascending=False).head(10)


# In[77]:


COUPON_RAW.head()


# In[78]:


PRODUCT_RAW.head()


# campaign 13,18,8 are the one with most product in them.

# In[79]:


coupon_product = COUPON_RAW.merge(right=PRODUCT_RAW,on='PRODUCT_ID',how='left')


# In[80]:


coupon_product


# Top 10 product on which the Coupon has been applied

# In[81]:


coupon_product['COMMODITY_DESC'].value_counts().head(10)


# Most prominent products among coupons created are bathroom products such as hair care and makeup.

# In[82]:


COUPON_REDEMPT_RAW.shape


# In[83]:


COUPON_REDEMPT_RAW.columns


# In[84]:


COUPON_REDEMPT_RAW['COUPON_UPC'].nunique()


# In[85]:


COUPON_REDEMPT_RAW.groupby('CAMPAIGN').agg(total_coupon_reedm=('COUPON_UPC','nunique')).sort_values(by='total_coupon_reedm',ascending=False).plot.bar()


# In[86]:


Coupon_redeem = COUPON_REDEMPT_RAW.groupby('CAMPAIGN').agg(total_coupon_reedm=('COUPON_UPC','nunique'))


# In[87]:


Coupon_redeem.sort_values(by = 'total_coupon_reedm',ascending=False)


# In[88]:


Coupon_Given.head(5)


# In[89]:


Coupon_redeem.head(5)


# In[90]:


Coupon_redeem = Coupon_redeem.merge(right = Coupon_Given,on='CAMPAIGN',how='left')


# In[91]:


Coupon_redeem.head()


# In[92]:


Coupon_redeem['Coupon_redeem_rate']=(Coupon_redeem['total_coupon_reedm']/Coupon_redeem['Total_Coupon_Given'])*100


# In[93]:


Coupon_redeem.head().sort_values(by = 'Coupon_redeem_rate',ascending = False)


# In[94]:


plt.figure(figsize=(15,5))
sns.barplot(x='CAMPAIGN',y='Coupon_redeem_rate',data=Coupon_redeem)


# In[95]:


TRANSACTION_RAW.shape


# In[96]:


TRANSACTION_RAW.columns


# In[97]:


TRANSACTION_RAW['BASKET_ID'].count()


# In[98]:


TRANSACTION_RAW['BASKET_ID'].nunique()


# In[99]:


TRANSACTION_RAW['HOUSEHOLD_KEY'].nunique()


# In[100]:


trnx_bucket =TRANSACTION_RAW.groupby('BASKET_ID').aggregate({'SALES_VALUE':'sum','COUPON_DISC':'sum','COUPON_MATCH_DISC':'sum'})


# In[101]:


trnx_bucket.head(10)


# In[102]:


trnx_bucket['Use_coupon'] = trnx_bucket['COUPON_DISC']!=0


# In[103]:


trnx_bucket['Use_coupon'].value_counts()


# In[104]:


trnx_bucket.sort_values('SALES_VALUE',ascending=False).head(10)


# In[105]:


round(trnx_bucket['SALES_VALUE'].mean(),2)


# In[106]:


plt.figure(figsize=(25,5))
sns.boxplot(x=trnx_bucket['SALES_VALUE'])
plt.title('Basket value boxplot', fontsize = 20)


# The average basket value without a coupon is $26.79.
# 
# The average basket value with a coupon is $68.21.
# 
# The average discount generated by coupons is $2.98.

# It shows that customeer purchase more product when coupon is given to them

# In[107]:


trnx_desc = TRANSACTION_RAW.merge(right= trnx_bucket,on='BASKET_ID',how='left')


# In[108]:


trnx_desc= trnx_desc.merge(right=PRODUCT_RAW,on='PRODUCT_ID',how='left')


# In[109]:


trnx_desc.head(3)


# In[110]:


trnx_desc.drop(['SALES_VALUE_y','COUPON_DISC_y','COUPON_MATCH_DISC_y'],axis=1,inplace=True)


# In[111]:


trnx_desc.head()


# In[112]:


trnx_bucket.groupby('Use_coupon').aggregate( sales_mean=('SALES_VALUE','mean'),
                                             COUPON_DISC_mean =('COUPON_DISC','mean'),
                                             COUPON_MATCH_DISC=('COUPON_MATCH_DISC','mean'),
                                             No_coupon  =('SALES_VALUE','count'))


# In[113]:


COMMODITY_Coupon = trnx_desc.groupby('COMMODITY_DESC').aggregate(total_quantity=('QUANTITY','count'),
                                             Use_coupon=('Use_coupon','sum'),
                                             Coupon=('COUPON_DISC_x','sum'))


# In[114]:


COMMODITY_Coupon.head(10)


# In[115]:


COMMODITY_Coupon['Coupon%']=round((COMMODITY_Coupon['Use_coupon']/COMMODITY_Coupon['total_quantity'])*100,2)


# In[116]:


COMMODITY_Coupon.sort_values('Coupon%',ascending=False).head(30)


# While the most prominents products for which coupons are created are haircare and makeup products, coupons are mostly used on drinks, cigarettes, diapers, etc. Bathroom products are not even among the top 10

# In[117]:


TRANSACTION_RAW.groupby(['HOUSEHOLD_KEY','WEEK_NO','DAY']).aggregate({'SALES_VALUE':'sum','RETAIL_DISC':'sum',
                                                                  'COUPON_DISC' :'sum','COUPON_MATCH_DISC':'sum'})


# In[118]:


TRANSACTION_RAW.head()


# In[119]:


TRANSACTION_RAW.groupby(TRANSACTION_RAW['DATE'].dt.year).aggregate({'SALES_VALUE':'sum','RETAIL_DISC':'sum',
                                                                   'COUPON_DISC':'sum'})


# Sales Value increases as Retail Discount and Coupon Discount increases

# # Data Transformation

# Dropping the Column

# In[120]:


from datetime import datetime


# In[121]:


CAMPAIGN_DESC_RAW.drop(['START_DAY','END_DAY'],axis=1,inplace=True)


# In[122]:


CAMPAIGN_DESC_RAW.head(3)


# In[124]:


CAMPAIGN_DESC_RAW['Start_date'] = pd.to_datetime(CAMPAIGN_DESC_RAW['Start_date']).apply(lambda x: x.date())


# In[125]:


type(CAMPAIGN_DESC_RAW['Start_date'])


# In[126]:


CAMPAIGN_DESC_RAW['Start_date'] 


# In[127]:


CAMPAIGN_DESC_RAW['End_date'] = pd.to_datetime(CAMPAIGN_DESC_RAW['End_date']).apply(lambda x: x.date())


# In[128]:


type(CAMPAIGN_DESC_RAW['End_date'])


# In[129]:


CAMPAIGN_DESC_RAW['End_date'] 


# In[130]:


CAMPAIGN_DESC_RAW.dtypes


# In[123]:


COUPON_REDEMPT_RAW.drop(['DAY'],axis = 1, inplace = True)


# In[131]:


COUPON_REDEMPT_RAW.head(3)


# In[133]:


COUPON_REDEMPT_RAW['Date'] = pd.to_datetime(COUPON_REDEMPT_RAW['Date']).apply(lambda x: x.date())


# In[134]:


COUPON_REDEMPT_RAW.dtypes


# In[144]:


COUPON_REDEMPT_RAW.head(10)


# In[135]:


TRANSACTION_RAW.drop(['DAY','WEEK_NO'],axis=1,inplace=True)


# In[136]:


TRANSACTION_RAW.head()


# In[141]:


TRANSACTION_RAW['DATE']=pd.to_datetime(TRANSACTION_RAW['DATE']).apply(lambda x: x.date())


# In[142]:


TRANSACTION_RAW.dtypes


# In[143]:


TRANSACTION_RAW.head(10)


# # Now loading the table to Database

# In[145]:


from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import snowflake.connector as snowCtx
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import getpass


# In[146]:


conn = snowflake.connector.connect(
       user = 'VHARSHBHARDWAJ',
       password = getpass.getpass('Your Snowflake Password: '),
       ##password='((whatever your password is))',
       ##  account = https://uyvaacf-ic87213.snowflakecomputing.com/
       account = 'uyvaacf-ic87213',
       database='RETAILS',
       schema='PUBLIC',
       warehouse='COMPUTE_WH',
 ) 


# In[147]:


cur=conn.cursor()


# In[150]:


cur.execute('''
CREATE OR REPLACE TABLE CAMPAIGN_DESC_NEW
(DESCRIPTION VARCHAR(10),
CAMPAIGN NUMBER(38,0),
Start_date date,
End_date  date,
Campaign_Duration NUMBER(38,0),
Start_month VARCHAR(10),
End_month VARCHAR(10),
Start_Year INT,
End_Year INT)''')


# In[148]:


cur.execute(''' CREATE OR REPLACE TABLE COUPON_REDEMPT_NEW
(HOUSEHOLD_KEY NUMBER(38,0),
COUPON_UPC NUMBER(38,0),
CAMPAIGN NUMBER(38,0),
Date Date
)''')


# In[149]:


cur.execute('''CREATE OR REPLACE TABLE TRANSACTION_NEW
(HOUSEHOLD_KEY NUMBER(38,0),
BASKET_ID NUMBER(38,0),
PRODUCT_ID NUMBER(38,0),
QUANTITY NUMBER(38,0),
SALES_VALUE FLOAT,
STORE_ID NUMBER(38,0),
RETAIL_DISC FLOAT,
TRANS_TIME NUMBER(38,0),
COUPON_DISC FLOAT,
COUPON_MATCH_DISC FLOAT,
Date Date
)
''')


# In[151]:


success, nchunks, nrows, _ = write_pandas(conn, CAMPAIGN_DESC_RAW,'CAMPAIGN_DESC_NEW',quote_identifiers=False)
print(str(success)+','+str(nchunks)+','+str(nrows))


# In[152]:


success, nchunks, nrows, _ = write_pandas(conn, COUPON_REDEMPT_RAW,'COUPON_REDEMPT_NEW',quote_identifiers=False)
print(str(success)+','+str(nchunks)+','+str(nrows))


# In[153]:


success, nchunks, nrows, _ = write_pandas(conn,TRANSACTION_RAW ,'TRANSACTION_NEW',quote_identifiers=False)
print(str(success)+','+str(nchunks)+','+str(nrows))


# In[ ]:


cur.close()
conn.close()


# In[154]:


pip install jupyterlab-scheduler


# In[ ]:





# In[ ]:




