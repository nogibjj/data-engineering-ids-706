import secrets

kaggle_dataset = "jacksoncrow/stock-market-dataset"

server = 'ids-706-assignment-3.database.windows.net'
database = 'ids-706'
username = 'adjohn-admin'
password = secrets.password
driver= '{ODBC Driver 18 for SQL Server}'
data_directory = '/home/adjohn/ids-706/Assignment-3/stock_market_data/nyse/csv'
table_name = 'stock_data'