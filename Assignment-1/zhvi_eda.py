#%%
"""
Data Engineering - Assignment 1
"""
from datetime import datetime
import wget
from pyspark.sql import SparkSession

# Entry point for PySpark - creating a SparkSession
spark = SparkSession.builder.master("local[2]").appName("Zillow Home Value Index EDA").getOrCreate()

file_location = "/workspaces/data-engineering-ids-706/Assignment-1/zhvi_data.csv"
rdd = spark.read.option("header",True).csv(file_location)
# %%
def filter_by_state(rdd, state_code='all'):
    """
    Returns the RDD filtered by country name
    """
    if len(state_code) > 2 or state_code.islower():
        return {"error": "Invalid state code. Please enter a valid state code of the form NY.", 
        "result":None}
    if state_code == 'all':
        return {"error": None, "result": rdd}
    
    else:
        result= rdd.filter(rdd['StateName'] == state_code)
        return {"error": None, "result": result}
country_data = filter_by_state(rdd, 'NY')

print(country_data['result'].show(3, vertical=True))


# %%
def filter_by_year(rdd, start_date, end_date):
    """
    Returns the RDD filtered by year
    """
    if start_date == None and end_date == None:
        return {"error":None, "result":rdd}
    try:
        str_cols = ['RegionID', 'SizeRank', 'RegionName', 'StateName', 'RegionType']
        date_cols = list(set([col[0] for col in rdd.dtypes]) - set(str_cols))
        date_cols = [datetime.strptime(col, '%Y-%m-%d') for col in date_cols]
    
    except Exception as ee:
        return {"error": ee, "result": None}
    
    if start_date == None:
        date_cols = [col.strftime('%Y-%m-%d') for col in date_cols if col <= end_date]
    
    elif end_date == None:
        date_cols = [col.strftime('%Y-%m-%d') for col in date_cols if col >= start_date]
    
    else:
        date_cols = [col.strftime('%Y-%m-%d') for col in date_cols if col >= start_date and col <= end_date]
    
    rdd = rdd.select(*[date_cols])
    return {"error": None, "result": rdd} 
    
year_data = filter_by_year(rdd, start_date=None, end_date=datetime.strptime('2005-01-01', '%Y-%m-%d'))
print(year_data['result'].show(3, vertical=True))

#%%
def download_file():
    url = 'https://files.zillowstatic.com/research/public_csvs/zhvi/State_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1662605668'
    download_location = '/workspaces/data-engineering-ids-706/Assignment-1/zhvi_data.csv'
    wget.download(url,out = download_location)
download_file()
# %%
def perform_operations(rdd, operation):
    if operation == 'min':
        result = rdd.select([min(c) for c in rdd.columns])
        return {"error": None, "result": result}
    
    elif operation == 'max':
        result = rdd.select([max(c) for c in rdd.columns])
        return {"error": None, "result": result}
    
    elif operation == 'mean':
        result = rdd.select([mean(c) for c in rdd.columns])
        return {"error": None, "result": result}

    elif operation == 'median':
        result = rdd.select([median(c) for c in rdd.columns])
        return {"error": None, "result": result}    

    elif operation == 'std':
        result = rdd.select([stddev(c) for c in rdd.columns])
        return {"error": None, "result": result}

    elif operation == 'var':
        result = rdd.select([variance(c) for c in rdd.columns])
        return {"error": None, "result": result}
    
    else:
        return {"error": "Invalid operation", "result": None}


perform_operations(rdd, 'min')
# %%
