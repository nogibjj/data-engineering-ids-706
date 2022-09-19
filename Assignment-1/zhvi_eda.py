"""
Data Engineering - Assignment 1 
Analyze the Zillow House Value Index (ZHVI) data set to identify the top 3 states with the biggest housing bubble
"""
#%%
from datetime import datetime as dt
import sys
import traceback
from pyspark.sql import SparkSession, functions as f
from matplotlib import pyplot as plt
import numpy as np
from functions import melt_rdd, calculate_percent_change, return_chart_data

"""
Creating the spark session
"""
try:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("Zillow Home Value Index EDA")
        .getOrCreate()
    )
except Exception as ee:
    print("Error creating SparkSession")
    print(traceback.format_exc())
    sys.exit(1)

"""
Important dates - Rounded to end of month to match the ZHVI data
"""
covid_date = dt(2020, 1, 31)
interest_hike_date = dt(2022, 2, 28)

"""
Reading the input files - Zillow Home Value Index (ZHVI)
"""
try:
    zhvi_data_loc = "/workspaces/data-engineering-ids-706/Assignment-1/zhvi_data.csv"
    zhvi_rdd = spark.read.option("header", True).csv(zhvi_data_loc)
    print(zhvi_rdd.printSchema())
except Exception as ee:
    print("Error reading/transforming input data")
    print(traceback.format_exc())
    sys.exit(1)

"""
The following transformations are performed on the ZHVI RDD:
    - Covert the data from wide to long format using the "melt_rdd" function
    - Calculate the percent change in the ZHVI value using the "calculate_percent_change" function
"""
try:
    zhvi_str_cols = ["RegionID", "SizeRank", "RegionName", "StateName", "RegionType"]
    zhvi_date_cols = list(set([col[0] for col in zhvi_rdd.dtypes]) - set(zhvi_str_cols))
    zhvi_rdd = melt_rdd(
        zhvi_rdd,
        id_vars=zhvi_str_cols,
        value_vars=zhvi_date_cols,
        var_name="date",
        value_name="value",
    )

    zhvi_rdd = zhvi_rdd.withColumn("date", f.to_date(zhvi_rdd.date, "M/d/y"))
    zhvi_rdd = calculate_percent_change(
        zhvi_rdd, partition_col="RegionID", value_col="value", date_col="date"
    )
    print(zhvi_rdd.printSchema())
except Exception as ee:
    print("Error in applying transformations on the ZHVI RDD")
    print(traceback.format_exc())
    sys.exit(1)

"""
Analyse the price change after Fed began hiking interest rates
    - Filter to only include house prices post interest hike
    - Calculate slope of best fit line to determine the average price change
    - Store results in a dictionary
"""
all_states = zhvi_rdd.select("StateName").distinct().collect()
all_states = [x["StateName"] for x in all_states]
results_dict = {}

for state in all_states:
    try:
        state_rdd = zhvi_rdd.where(
            (zhvi_rdd.StateName == state) & (zhvi_rdd.date >= interest_hike_date)
        )
        state_rdd = state_rdd.sort("date")
        state_values = [
            float(x[0])
            for x in state_rdd.select("percent_change").collect()
            if x[0] is not None
        ]
        state_dates = [x[0] for x in state_rdd.select("date").collect()]
        slope, intercept = np.polyfit(x=state_values, y=range(len(state_dates)), deg=1)
        results_dict[state] = slope
    except Exception as ee:
        print("Error calculating slope for state: {}".format(state))
        print(traceback.format_exc())
        sys.exit(1)

"""
Plot the full price history for the top 3 states with the biggest rate of drop in prices after the interest hike
"""
higest_drop_states = sorted(results_dict.items(), key=lambda x: x[1])[:3]
#%%
for state in higest_drop_states:
    (
        pre_date_dates,
        pre_date_values,
        post_date_dates,
        post_date_values,
    ) = return_chart_data(rdd=zhvi_rdd, date=interest_hike_date, state=state[0])
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.plot(
        pre_date_dates,
        pre_date_values,
        color="deepskyblue",
        label="pre-hike housing values",
    )
    ax.plot(
        post_date_dates, post_date_values, color="red", label="post-hike housing values"
    )
    ax.set_xlabel("Date")
    ax.set_ylabel("Percentage Change in House Value - Month over Month")
    fig.suptitle(f"House prices for state {state[0]}", fontsize=16)
    plt.show()

# %%
