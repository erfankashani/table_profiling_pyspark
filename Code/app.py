#!/usr/bin/env python
# coding: utf-8

# In this notebook we aim to create a general spark script to perform initial table profiling. the tasks involve:
# - Add the data as RDD
# - Create a table with the column names and the count of each 
# - Devide columns into numeric vs none-nummeric
# - some of the features for nun-numeric:
#     - Record count
#     - Record Count
#     - Unique Values
#     - Empty Strings
#     - Null Values
#     - Percent Fill
#     - Percent Numeric
#     - Max Length
# 
# - create a file_to_be_profiled, that can handle csv,tsv,delta,gz data formats and in a for loop profile all
# - Create a front-end for the results

# ### Add Libraries

# In[1]:


import os
import sys


# In[2]:


# Pyspark sessions
from pyspark.sql import SparkSession


# In[3]:


from pyspark.sql import functions as F


# #### Create spark session

# In[4]:


spark = SparkSession.builder.appName("SparkProfiling").getOrCreate()
spark


# In[5]:


# if you need the sparkConext
sc=spark.sparkContext
sc


# In[6]:


# If folder results does not exist create one
if not os.path.exists('../Result'):
    os.makedirs('../Result')


# In[7]:


# List data inputs ready to use
files=os.listdir('../Data')
files


# In[8]:


def count_not_null(c, nan_as_null=False):
    pred = F.col(c).isNotNull() & (~isnan(c) if nan_as_null else F.lit(True))
    return F.sum(pred.cast("integer")).alias(c)


# In[50]:


def get_top_five_frequent_record(col):
    frequency_dataframe=DF.groupBy(col).count().sort(F.desc('count'))
    frequency_dataframe=frequency_dataframe.where(F.col(col).isNotNull())
    top_frequency_five=[]
    if frequency_dataframe.count()<5:
        top_frequency_five=[row[0] for row in frequency_dataframe.collect()]
    else:
        top_frequency_five=[row[0] for row in frequency_dataframe.take(5)]
    return top_frequency_five


# In[9]:


# read the dataframe
filepath='../Data/'+files[0]
DF = spark.read.format('csv').options(header='true',inferschema='true').load(filepath)
DF.show(5)


# In[10]:


columns_names=DF.columns
columns_names


# In[11]:


total_rows=DF.count()
total_rows


# In[55]:


# for i, x in enumerate(DF.columns):
#     DF=DF.withColumnRenamed(x, str(i))
# DF.show(5)


# In[12]:


compute_not_null_columns = DF.agg(*[count_not_null(c) for c in DF.columns]).collect()[0]
compute_not_null_columns


# In[57]:


compute_null_columns=[(total_rows-count_notNull) for count_notNull in compute_not_null_columns]
compute_null_columns


# In[60]:


compute_null_proportion=[round((count_Nulls / total_rows)*100, 3) for count_Nulls in compute_null_columns]
compute_null_proportion


# In[61]:


cols_data=[]

for i, cols in enumerate(DF.columns):
    if total_rows==0:
        continue
    columns_data={}
    columns_data['column_name']=cols
    columns_data['dtype']= DF.dtypes[i][1]
    columns_data['record_count']=total_rows
    columns_data['number_non_empty_cells']=compute_not_null_columns[i]
    columns_data['number_empty_cells']=compute_null_columns[i]
    columns_data['null_proportion']=compute_null_proportion[i]
    columns_data['top_five_value'] = get_top_five_frequent_record(cols)
    cols_data.append(columns_data)


# In[62]:


cols_data


# In[63]:


spark_df = spark.read.json(sc.parallelize(cols_data))
spark_df.show()


# In[28]:


columns_names[1]


# In[65]:


pandas_df = spark_df.toPandas()
pandas_df = pandas_df.set_index("column_name")
pandas_df


# In[66]:


import streamlit as st
import pandas as pd

st.write(pandas_df)

'''
# Creating Web Apps with JN and Streamlit
## Erfan Kashani
christianfjung.com

**About this Project** 

This project uses live poll data from 538 so the website will be update constantly!

'''


# In[72]:




# In[70]:




# In[ ]:




