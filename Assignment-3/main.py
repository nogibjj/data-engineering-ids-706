import os
import warnings
import pandas as pd

from helper_functions import *
from config import *

warnings.filterwarnings("ignore")

def create_azure_sql_table():
    conn = connect_azure_sql()
    drop_table(conn)
    create_table(conn)
    conn.close()

def insert_data(data_directory, limit=config.ticker_insert_limit):
    conn = connect_azure_sql()
    cursor = conn.cursor()
    for file in os.listdir(data_directory)[0:limit]:
        print("inserting data file: ", file)
        df = pd.read_csv(os.path.join(data_directory, file))
        df['Date'] = pd.to_datetime(df['Date'])
        for row in df.iterrows():
            cursor.execute(
                f"""
                INSERT INTO {config.table_name} VALUES (
                    '{file.split('.csv')[0]}',
                    '{row[1][0]}',
                    '{row[1][1]}',
                    {row[1][2]},
                    {row[1][3]},
                    {row[1][4]},
                    {row[1][5]},
                    {row[1][6]}
                )
                """
            )
        print("Completed inserting file: ", file)
        conn.commit()
    print("Successfully inserted all data into Azure SQL")
    conn.close()

def get_trend_value():
    conn = connect_azure_sql()
    cursor = conn.cursor()
    all_tickers = cursor.execute(f"SELECT DISTINCT symbol FROM {config.table_name}").fetchall()
    all_tickers = [ticker[0] for ticker in all_tickers]
    print(all_tickers)
    user_ticker = input("Enter a ticker to get the trend value: ")
    if user_ticker in all_tickers:
        cursor.execute(regression_query.format(user_ticker))
        rows = cursor.fetchall()
        print(rows)
    else:
        print("Ticker not found in database. Please try again.")

if __name__ == "__main__":
    #create_azure_sql_table()
    #insert_data("/workspaces/data-engineering-ids-706/Assignment-3/stock_market_data/nasdaq/csv")
    get_trend_value()