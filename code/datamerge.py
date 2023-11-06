#!/usr/bin/env python
# coding: utf-8

# In[62]:


import pandas as pd
import copy
import os
import sys


# In[2]:


from s3_bucket_access import *


# In[54]:


import time
def get_file_name(folderName, version):
  timestr = time.strftime("%Y%m%d%H%M%S")
  file_name = 'cac_'+folderName+'_'+timestr+'_'+version+'.csv'

  return file_name


# In[4]:


download_objects(s3_bucket_name, from_path=remote(DirType.DIR_TYPE_RAW), to_path=local(DirType.DIR_TYPE_RAW))


# In[14]:


path = '/Users/sahana/Desktop/Sem2/DataAnalyticProcesses/code/raw_datasets/'
customer = pd.read_csv(path+'customer.csv')
product = pd.read_csv(path+'product.csv')
product_class = pd.read_csv(path+'product_class.csv')
promotion = pd.read_csv(path+'promotion.csv')
region = pd.read_csv(path+'region.csv')
sales = pd.read_csv(path+'sales.csv')
store = pd.read_csv(path+'store.csv')
time_by_day = pd.read_csv(path+'time_by_day.csv')


# In[15]:


# removing nominal columns with missing columns, 3 columns > 10% missing and 1 column > 4.3% missing
customer.drop(columns=['mi','address2','address3','address4'], inplace = True)
# dropping other nominal and ordinal columns that may not contribute to the analysis
customer.drop(columns=['account_num','lname','fname','fullname','address1','city','state_province','postal_code','country','customer_region_id','phone1','phone2','birthdate','date_accnt_opened'], inplace = True)


# In[16]:


# dropping few columns that are not relevant
product.drop(columns=['SKU','product_name','cases_per_pallet','shelf_width','shelf_height','shelf_depth'], inplace = True)


# In[17]:


# dropping the subcategory column as the division. becomes too granular
product_class.drop(columns=['subcategory'], inplace = True)


# In[18]:


# lets combine two tables, product and product_class
product_and_class = pd.merge(product, product_class, on = 'product_class_id', how = 'inner')


# In[19]:


# lets drop product class id column as it is no longer required
product_and_class.drop(columns=['product_class_id'], inplace=True)


# In[20]:


# droppping some ordinal and nominal columns in promotion table
promotion.drop(columns=['promotion_district_id','start_date','end_date'], inplace=True)


# In[21]:


# dropping few nominal columns as they are not relevant
region.drop(columns=['sales_city','sales_state_province','sales_district','sales_region','sales_district_id'], inplace=True)


# In[22]:


# a row which does not map to promotion is not useful as we are concerned on the promotions
print(sales.shape[0] - sales['promotion_id'].isna().sum())


# In[23]:


sales.dropna(inplace=True)


# In[24]:


# since there are only 25 rows in store table, we cannot drop the missing row columns
# first lets impute numerical columns by their medians since they are not affected by outliers

store['store_sqft'] = store['store_sqft'].fillna(store['store_sqft'].median())
store['grocery_sqft'] = store['grocery_sqft'].fillna(store['grocery_sqft'].median())
store['frozen_sqft'] = store['frozen_sqft'].fillna(store['frozen_sqft'].median())
store['meat_sqft'] = store['meat_sqft'].fillna(store['meat_sqft'].median())
# lets impute categorical variables using mode
store['store_manager'] = store['store_manager'].fillna(store['store_manager'].mode())
store['store_phone'] = store['store_phone'].fillna(store['store_phone'].mode())
store['store_fax'] = store['store_fax'].fillna(store['store_fax'].mode())
store['first_opened_date'] = store['first_opened_date'].fillna(store['first_opened_date'].mode())
store['last_remodel_date'] = store['last_remodel_date'].fillna(store['last_remodel_date'].mode())


# In[25]:


# lets drop few ordinal and nominal columns that are not required for our analysis
store.drop(columns=['store_name','store_number','store_street_address','store_postal_code','store_country','store_manager','store_phone','store_fax','first_opened_date','last_remodel_date'], inplace=True)


# In[26]:


# lets combine store and region tables since region Id is present in store table and store Id is present in sales table
store_and_region = pd.merge(store, region, on = 'region_id', how = 'inner')


# In[27]:


store_and_region.drop(columns=['region_id'],inplace=True)


# In[28]:


# first lets merge sales with product table
sales_product = pd.merge(sales, product_and_class, on = 'product_id', how = 'inner')


# In[29]:


# lets merge sales_product dataframe with promotion dataframe
sales_prod_promo = pd.merge(sales_product, promotion, on = 'promotion_id', how = 'inner')


# In[30]:


# lets merge sales_prod_promo dataframe with store_and_region dataframe
sales_prod_promo_store = pd.merge(sales_prod_promo, store_and_region, on = 'store_id', how = 'inner')


# In[32]:


# lets merge sales_prod_promo_store dataframe with store_and_region dataframe
sales_prod_promo_store_cust = pd.merge(sales_prod_promo_store, customer, on = 'customer_id', how = 'inner')


# In[33]:


unique_combinations1 = sales_prod_promo_store_cust[['promotion_id','customer_id','cost']]


# In[35]:


test = copy.copy(unique_combinations1[['promotion_id','cost']].drop_duplicates().sort_index())


# In[36]:


test2 = unique_combinations1.groupby("promotion_id")['customer_id'].count().reset_index()


# In[37]:


test2 = test2.rename(columns={'customer_id': 'customer_count'})


# In[38]:


test3 = pd.merge(test, test2, left_on='promotion_id', right_on='promotion_id', how='inner')


# In[39]:


test3['cost_per_customer'] = test3['cost']/test3['customer_count']


# In[40]:


test3.drop(columns=['cost','customer_count'],inplace=True)


# In[41]:


customer_acq_cost = pd.merge(sales_prod_promo_store_cust, test3, on='promotion_id', how='inner')


# In[42]:


# lets drop the nominal id columns
customer_acq_cost.drop(columns=['product_id','time_id','customer_id','promotion_id','store_id','cost'], inplace=True)


# In[43]:


customer_acq_cost['cost_per_customer'] = customer_acq_cost['cost_per_customer'].astype(float).round(2)


# In[47]:


customer_acq_cost = customer_acq_cost.rename(columns={'store_sales':'store_sales(in millions)','store_cost':'store_cost(in millions)','unit_sales':'unit_sales(in millions)','category':'food_category',\
                                                      'department':'food_department','family':'food_family','cost_per_customer':'cost'})


# In[48]:


new_order = ['food_category', 'food_department', 'food_family','store_sales(in millions)', 'store_cost(in millions)', 'unit_sales(in millions)', 'promotion_name', 'sales_country',\
             'marital_status', 'gender', 'total_children', 'education', 'member_card', 'occupation', 'houseowner', 'num_cars_owned', 'yearly_income', 'num_children_at_home',\
             'brand_name', 'SRP', 'gross_weight', 'net_weight', 'recyclable_package', 'low_fat', 'units_per_case', 'store_type', 'store_city', 'store_state', 'store_sqft', 'grocery_sqft',\
             'frozen_sqft', 'meat_sqft', 'coffee_bar', 'video_store', 'salad_bar', 'prepared_food', 'florist', 'media_type', 'cost']
CAC = customer_acq_cost[new_order]


# In[53]:


list_object(s3_bucket_name, from_path=remote(DirType.DIR_TYPE_MERGED))


# In[59]:


version = sys.argv[1]
filename = get_file_name('merged',version)
merge_path = '/Users/sahana/Desktop/Sem2/DataAnalyticProcesses/code/merged_data/'
if(not(check_if_local_dir_exists(merge_path))):
    os.makedirs(merge_path)


# In[60]:


CAC.to_csv(merge_path+filename)


# In[61]:


upload_objects(s3_bucket_name, from_path=local(DirType.DIR_TYPE_MERGED), to_path=remote(DirType.DIR_TYPE_MERGED))


# In[ ]:




