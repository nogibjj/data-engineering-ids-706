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

if __name__ == "__main__":
    conn = connect_azure_sql()
    drop_table(conn)
    create_table(conn)
    conn.close()
