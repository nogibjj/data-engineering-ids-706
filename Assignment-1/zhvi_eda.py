"""
Data Engineering - Assignment 1
"""
# %%
from pyspark.sql import SparkSession

# %%
# Entry point for PySpark
spark = SparkSession.builder.master("local[2]")\
    .appName("Zillow Home Value Index EDA").getOrCreate()

# %%
rdd = spark.read.option("header",True) \
    .csv('zhvi_data.csv')
rdd.printSchema()


# %%
rdd.show(n=5, vertical=True)
# %%
