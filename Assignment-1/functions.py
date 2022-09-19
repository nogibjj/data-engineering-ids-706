from pyspark.sql.functions import array, col, explode, lit, struct, lag
from pyspark.sql import DataFrame
from typing import Iterable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

"""
Function to melt the RDD into a long format

    :param df: The RDD to be melted
    :param id_vars: The columns to be kept as is
    :param value_vars: The columns to be melted
    :param var_name: The name of the column containing the melted column names
    :param value_name: The name of the column containing the melted values
    
    :return: The melted RDD

Referencing Stack Overflow
zero323's answer on https://stackoverflow.com/questions/41670103/how-to-melt-spark-dataframe
"""
def melt_rdd(
    df: DataFrame,
    id_vars: Iterable[str],
    value_vars: Iterable[str],
    var_name: str = "variable",
    value_name: str = "value",
) -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(
        *(struct(lit(c).alias(var_name), col(c).alias(value_name)) for c in value_vars)
    )

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)

"""
Function to calculate the percent change

    :param rdd: Input RDD
    :param partition_col: The column to partition by
    :param value_col: The column to calculate the percent change for
    :param date_col: The column containing the date
    
    :return: The RDD with the percent change calculated for value in the parition column
"""
def calculate_percent_change(
    rdd: DataFrame, partition_col: str, value_col: str, date_col: str
) -> DataFrame:
    window = Window.partitionBy(partition_col).orderBy(partition_col, date_col)
    rdd = rdd.withColumn("prev_value", F.lag(rdd[value_col]).over(window))
    rdd = rdd.withColumn(
        "diff",
        F.when(F.isnull(rdd[value_col] - rdd.prev_value), 0).otherwise(
            rdd[value_col] - rdd.prev_value
        ),
    )
    rdd = rdd.withColumn(
        "percent_change",
        F.when(F.isnull(rdd.diff / rdd.prev_value), 0).otherwise(
            100 * (rdd.diff / rdd.prev_value)
        ),
    )
    return rdd

"""
Function to format the input RDD into a format that can be used by the charting library
    :param rdd: Input RDD
    :param date: The date to filter the RDD on
    :param state: The state to filter the RDD on

    :return: The RDD formatted for the charting library
"""
def return_chart_data(rdd, date, state):
    state_rdd = rdd.where(rdd.StateName == state)
    pre_date_rdd = state_rdd.where(state_rdd.date <= date)
    pre_date_rdd = pre_date_rdd.sort("date")
    pre_date_values = [
        float(x[0])
        for x in pre_date_rdd.select("percent_change").collect()
        if x[0] is not None
    ]
    pre_date_dates = [x[0] for x in pre_date_rdd.select("date").collect()]

    post_date_rdd = state_rdd.where(state_rdd.date >= date)
    post_date_rdd = post_date_rdd.sort("date")
    post_date_values = [
        float(x[0])
        for x in post_date_rdd.select("percent_change").collect()
        if x[0] is not None
    ]
    post_date_dates = [x[0] for x in post_date_rdd.select("date").collect()]

    return pre_date_dates, pre_date_values, post_date_dates, post_date_values
