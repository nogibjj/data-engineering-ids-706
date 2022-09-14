#%%
"""
Data Engineering - Assignment 1 
Analyse the ZHVI data to find how housing prices across states have kept pace with inflation.
"""
#%%
from datetime import datetime as dt
import sys
import traceback
import pandas as pd
from pyspark.sql import SparkSession, functions
from matplotlib import pyplot as plt
"""
Reading the input files - Zillow Home Value Index (ZHVI) and Inflation Data
"""
zhvi_data_loc = '/workspaces/data-engineering-ids-706/Assignment-1/zhvi_data.csv'
inflation_data_loc = '/workspaces/data-engineering-ids-706/Assignment-1/inflation_data.csv'

# Creating SparkSession
try:
    spark = SparkSession.builder.master("local[2]").appName("Zillow Home Value Index EDA").getOrCreate()
except Exception as ee:
    print("Error creating SparkSession")
    print(traceback.format_exc())
    sys.exit(1)
#%%
zhvi_rdd = spark.read.option("header",True).csv(zhvi_data_loc)
inflation_rdd = spark.read.option("header",True).csv(inflation_data_loc)
zhvi_str_cols =  ['RegionID', 'SizeRank', 'RegionName', 'StateName', 'RegionType']
zhvi_date_cols = list(set([col[0] for col in zhvi_rdd.dtypes]) - set(zhvi_str_cols))

for row in zhvi_rdd.take(50):
    region_name = row['RegionName']
    print(region_name)
    print([row[col[0]] for col in zhvi_rdd.dtypes if col[0] not in zhvi_str_cols])
    print([row[col[0]] for col in zhvi_rdd.dtypes if col[0] not in zhvi_str_cols])
    zhvi_values = [float(row[col[0]]) for col in zhvi_rdd.dtypes if col[0] not in zhvi_str_cols]
    zhvi_dates = [col[0] for col in zhvi_rdd.dtypes if col[0] not in zhvi_str_cols]
    zhvi_pcnt_change = pd.Series(zhvi_values).pct_change()*100
    inflation_dates = list(inflation_rdd.select('Year').toPandas()['Year'])
    inflation_values = list(inflation_rdd.select('Inflation Rate').toPandas()['Inflation Rate'])
    inflation_values = [float(val) for val in inflation_values]
    inflation_pcnt_change = pd.Series(inflation_values).pct_change()*100
    plt.title(f'ZHVI and Inflation for {region_name}')
    plt.plot(inflation_dates, inflation_values, label='Inflation')
    plt.plot(zhvi_dates, zhvi_pcnt_change, label='ZHVI')
    plt.legend()
    plt.show()
    temp =  pd.DataFrame({'Date':zhvi_dates, 'ZHVI':zhvi_pcnt_change, 'Inflation':inflation_values})

# %%
