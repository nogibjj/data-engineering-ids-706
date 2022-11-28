import pyodbc
import config

def connect_azure_sql():
    conn = pyodbc.connect(
        f'DRIVER={config.driver};'
        'SERVER=' + config.server + ';'
        'DATABASE=' + config.database + ';'
        'UID=' + config.username + ';'
        'PWD=' + config.password
    )
    return conn

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute(
        f"""
        CREATE TABLE {config.table_name} (
            symbol VARCHAR(10),
            date DATE,
            open_prc FLOAT,
            high_prc FLOAT,
            low_prc FLOAT,
            close_prc FLOAT,
            volume_prc FLOAT,
            adj_close_prc FLOAT
        )
        """
    )
    conn.commit()
    print(f"Created table -- {config.table_name}")
    print(f"""Schema for table -- {config.table_name}:
                        symbol VARCHAR(10),
                        date DATE,
                        open_prc FLOAT,
                        high_prc FLOAT,
                        low_prc FLOAT,
                        close_prc FLOAT,
                        volume_prc FLOAT,
                        adj_close_prc FLOAT
                        """)

def drop_table(conn):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {config.table_name}")
    conn.commit()
    print(f"Dropped table -- {config.table_name}")

regression_query = '''
select symbol, 1.0*sum((x-xbar)*(y-ybar))/sum((x-xbar)*(x-xbar)) as Beta
from
(
    select symbol,
        avg(adj_close_prc) over(partition by symbol) as ybar,
        adj_close_prc as y,
        avg(datediff(day,'20140102',[date])) over(partition by symbol) as xbar,
        datediff(day,'20140102',date) as x
    from [dbo].[stock_data] where symbol='{}'
) as Calcs
group by symbol
having 1.0*sum((x-xbar)*(y-ybar))/sum((x-xbar)*(x-xbar))>0
'''