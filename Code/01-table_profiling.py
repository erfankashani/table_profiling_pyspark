# Plugin: Table_profiling.py
# Purpose: Profiles the tables efficiently using pyspark
# Author: Erfan Kashani
########################################################################################

# ======================================================================================
# => Import Libraries
# ======================================================================================

import os
import sys
import json
from datetime import datetime
# Pyspark sessions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as D

# Create spark session
spark = SparkSession.builder.appName('SparkProfiling').getOrCreate()
sc = spark.sparkContext                # if you need the sparkConext

# ======================================================================================
# => Helper functions
# ======================================================================================

# Create directory
def create_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

# List the directory
def get_data_input_list(path):
    return os.listdir(path)

def count_not_null(c, nan_as_null=False):
    pred = F.col(c).isNotNull() & (~isnan(c) if nan_as_null else F.lit(True))
    return F.sum(pred.cast('integer')).alias(c)

def get_top_five_frequent_record(DF, col):
    frequency_dataframe=DF.groupBy(col).count().sort(F.desc('count'))
    frequency_dataframe=frequency_dataframe.where(F.col(col).isNotNull())
    top_frequency_five=[]
    if frequency_dataframe.count()<5:
        top_frequency_five=[row[0] for row in frequency_dataframe.collect()]
    else:
        top_frequency_five=[row[0] for row in frequency_dataframe.take(5)]
    return top_frequency_five

# This method, randomly samples 10% of the data and cache the samples to find the size. 
# This method is not acurate and can be costly.
def estimate_rdd_memory_size_mb(df):
    estimated_df = df.sample(fraction = 0.1)
    estimated_df.cache().foreach(lambda x: x)
    catalyst_plan = estimated_df._jdf.queryExecution().logical()
    test_kb = spark._jsparkSession.sessionState().executePlan(catalyst_plan).optimizedPlan().stats().sizeInBytes()
    return test_kb * 10 / (1024 * 1024)

# type cast validation functions
def validate_string_to_integer(d):
    if type(d)==str:
        try:
            z=int(d)
            return z
        except:
            return None
    else:
        return None
    
def validate_string_to_float(d):
    if type(d)==str:
        try:
            z=float(d)
            return z
        except:
            return None
    else:
        return None

def validate_date(d):
    try:
        z=parse(d)
        return str(z)
    except:
        return None

def profile_table(rdd_df):
    completed_profile = {}
    columns_names = rdd_df.columns
    total_rows = rdd_df.count()

    # Initial column related analysis
    compute_unique_values = rdd_df.agg(*[F.countDistinct(F.col(c)) for c in rdd_df.columns]).collect()[0]
    compute_not_null_columns = rdd_df.agg(*[count_not_null(c) for c in rdd_df.columns]).collect()[0]
    compute_null_columns=[(total_rows-count_notNull) for count_notNull in compute_not_null_columns]
    compute_null_proportion=[round((count_Nulls / total_rows)*100, 3) for count_Nulls in compute_null_columns]

    # general database analysis
    general_df_info = {}
    general_df_info["total_records"] = total_rows
    general_df_info["total_variables"] = len(columns_names)
    general_df_info["total_missing_percentage"] = sum(compute_null_columns) / (general_df_info["total_records"] * general_df_info["total_variables"])
    general_df_info["estimate_size_in_memory_Mb"] = estimate_rdd_memory_size_mb(rdd_df)
    general_df_info["average_record_size_in_memory_Mb"] = general_df_info["estimate_size_in_memory_Mb"] / total_rows
    completed_profile["general_db_info"] = general_df_info

    attribute_analysis = {}
    attribute_analysis['Numeric'] = len([item[0] for item in rdd_df.dtypes if (item[1].startswith('int') or item[1].startswith('float'))])
    attribute_analysis['Categorical'] = len([item[0] for item in rdd_df.dtypes if item[1].startswith('string')])
    attribute_analysis['Date'] = len([item[0] for item in rdd_df.dtypes if item[1].startswith('date')])
    completed_profile["attribute_analysis"] = attribute_analysis

    # UDF function for type casting
    get_int=F.udf(lambda x: x if type(x)==int else None, D.IntegerType())
    get_str=F.udf(lambda x: x if type(x)==str else None, D.StringType())
    get_flt=F.udf(lambda x: x if type(x)==float else None, D.FloatType())
    get_dt=F.udf(lambda x: validate_date(x), D.StringType())
    get_string_int=F.udf(lambda x: validate_string_to_integer(x), D.IntegerType())
    get_string_flt=F.udf(lambda x: validate_string_to_float(x), D.FloatType())

    # Column wide Analysis
    column_analysis = {}
    column_analysis['cols_data'] = []
    # cols_data=[]

    for i, cols in enumerate(rdd_df.columns):
        # Base case
        if total_rows==0:
            continue
        
        columns_data={}
        columns_data['column_name']=cols
        columns_data['dtype']= DF.dtypes[i][1]
        columns_data['record_count']=total_rows
        columns_data['unique_values']=compute_unique_values[i]
        columns_data['number_non_empty_cells']=compute_not_null_columns[i]
        columns_data['number_empty_cells']=compute_null_columns[i]
        columns_data['null_proportion']=compute_null_proportion[i]
        columns_data['top_five_value'] = get_top_five_frequent_record(rdd_df, cols)
        
        # Data type specific analysis (check for possible type casting)
        int_col=cols+' '+'int_type'
        str_col=cols+' '+'str_type'
        float_col=cols+ ' '+ 'float_type'
        date_col=cols+' '+'date_type'
        str_int_col=cols + ' '+'str_int'
        str_float_col=cols +' '+'str_float'
        
        df=rdd_df.select([get_int(cols).alias(int_col), 
                    get_str(cols).alias(str_col), 
                    get_flt(cols).alias(float_col), 
                    get_dt(cols).alias(date_col),
                    get_string_int(cols).alias(str_int_col),
                    get_string_flt(cols).alias(str_float_col)
                    ])
        
        int_df = df.select(int_col).where(F.col(int_col).isNotNull())
        str_df = df.select(str_col).where(F.col(str_col).isNotNull())
        float_df = df.select(float_col).where(F.col(float_col).isNotNull())
        date_df = df.select(date_col).where(F.col(date_col).isNotNull())
        str_int_df = df.select(str_int_col).where(F.col(str_int_col).isNotNull())
        str_float_df = df.select(str_float_col).where(F.col(str_float_col).isNotNull())
        
        columns_data['data_types']=[]
        
        if float_df.count()>1:
            type_data={}
            type_data['type']='REAL'
            type_data['count']=float_df.count()
            type_data['max_value']=float_df.agg({float_col: "max"}).collect()[0][0]
            type_data['min_value']=float_df.agg({float_col: "min"}).collect()[0][0]
            type_data['mean']=float_df.agg({float_col: "avg"}).collect()[0][0]
            type_data['stddev']=float_df.agg({float_col: 'stddev'}).collect()[0][0]
            columns_data['data_types'].append(type_data)

        if int_df.count()>1:
            type_data={}
            type_data['type']='INTEGER (LONG)'
            type_data['count']=int_df.count()
            type_data['max_value']=int_df.agg({int_col: 'max'}).collect()[0][0]
            type_data['min_value']=int_df.agg({int_col: 'min'}).collect()[0][0]
            type_data['mean']=int_df.agg({int_col: 'avg'}).collect()[0][0]
            type_data['stddev']=int_df.agg({int_col: 'stddev'}).collect()[0][0]
            columns_data['data_types'].append(type_data)

        if str_df.count()>1:
            type_data={'type':'TEXT', 'count': str_df.count()}
            str_rows=str_df.distinct().collect()
            str_arr=[row[0] for row in str_rows]
            if len(str_arr)<=5:
                type_data['shortest_values']=str_arr
                type_data['longest_values']=str_arr
            else:
                str_arr.sort(key=len, reverse=True)
                type_data['shortest_values']=str_arr[:-6:-1]
                type_data['longest_values']=str_arr[:5]

            type_data['average_length']=sum(map(len, str_arr))/len(str_arr) #this needs work since it is getting average length of the ditinct values not all values
            # also average length sometimes prints giberish ex"  {'name': 'Ronald Bruce Sith'"
            columns_data['data_types'].append(type_data)

        if date_df.count()>1:
            type_data={"type":"DATE/TIME", "count":date_df.count()}
            min_date, max_date = date_df.select(F.min(date_col), F.max(date_col)).first()
            type_data['max_value']=max_date
            type_data['min_value']=min_date
            columns_data['data_types'].append(type_data)

        if str_float_df.count()>1:
            type_data={}
            type_data['type']='REAL'
            type_data['count']=str_float_df.count()
            type_data['max_value']=str_float_df.agg({str_float_col: "max"}).collect()[0][0]
            type_data['min_value']=str_float_df.agg({str_float_col: "min"}).collect()[0][0]
            type_data['mean']=str_float_df.agg({str_float_col: "avg"}).collect()[0][0]
            type_data['stddev']=str_float_df.agg({str_float_col: 'stddev'}).collect()[0][0]
            columns_data['data_types'].append(type_data)

        if str_int_df.count()>1:
            type_data={}
            type_data['type']='INTEGER (LONG)'
            type_data['count']=str_int_df.count()
            type_data['max_value']=str_int_df.agg({str_int_col: 'max'}).collect()[0][0]
            type_data['min_value']=str_int_df.agg({str_int_col: 'min'}).collect()[0][0]
            type_data['mean']=str_int_df.agg({str_int_col: 'avg'}).collect()[0][0]
            type_data['stddev']=str_int_df.agg({str_int_col: 'stddev'}).collect()[0][0]
            columns_data['data_types'].append(type_data)
        column_analysis['cols_data'].append(columns_data)
    
    completed_profile["column_analysis"] = column_analysis

    return(completed_profile)

# Saves the Json file
def save_json(json_file,path):
    with open(path, 'w') as outfile:
        json.dump(json_file, outfile)

# ======================================================================================
# => Prepare Environment
# ======================================================================================

# If folder 'Result' does not exist, create one
create_dir('../Result') 

# List data inputs ready to use
files = get_data_input_list('../Data')
print("the files available for profiling in Data directory:")
print(files)

# debug: get user input
print(10*'_')
for idx, file in enumerate(files):
    print(idx, ':', file)
input_id = int(input('please choose the pofiling file_index:'))

# Choose the dataset file (in case csv)
profiling_input = files[input_id]             # choose the input file you want
print("The current profiling table:", profiling_input)

# ======================================================================================
# => Start Profiling
# ======================================================================================

# read the dataframe
filepath='../Data/'+profiling_input
DF = spark.read.format('csv').options(header='true',inferschema='true').load(filepath)
profiling_result = profile_table(DF)

# ======================================================================================
# => Store Results
# ======================================================================================

profiling_input_name = str(profiling_input).split('.')[0]

# Save the Json file
date = datetime.now().strftime("%Y_%m_%d-%I_%M_%p")
file_name = f"../Result/{profiling_input_name}_profiled_table_{date}"
save_json(profiling_result,f"{file_name}.json")

# make CSV file
spark_df = spark.read.json(sc.parallelize(profiling_result["column_analysis"]["cols_data"]))
data_exploded = spark_df.select('column_name', 
                                'dtype', 
                                'record_count',
                                'number_non_empty_cells',
                                'number_empty_cells',
                                'null_proportion',
                                'unique_values',
                                'top_five_value',
                                F.explode('data_types').alias('data_types')
                               ) 

data_exploded = data_exploded.select('column_name', 
                                     'dtype',
                                     'record_count',
                                     'number_non_empty_cells',
                                     'number_empty_cells',
                                     'null_proportion',
                                     'unique_values',
                                     'top_five_value', 
                                     'data_types.*'
                                    )          
# Write the CSV file
data_exploded.withColumn("top_five_value", F.col("top_five_value").cast("string"))   \
             .withColumn("longest_values", F.col("longest_values").cast("string"))   \
             .withColumn("shortest_values", F.col("shortest_values").cast("string")) \
             .coalesce(1)                                                            \
             .write.option("header",True).csv(file_name)

# Stop spark session
spark.stop()