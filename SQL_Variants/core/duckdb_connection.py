import duckdb


def get_connection(db_path=':memory:'):
    return duckdb.connect(database=db_path)


def register_table(con, table_name, csv_path):
    con.execute(f"""
        CREATE TEMP TABLE {table_name} AS 
        SELECT * FROM read_csv_auto('{csv_path}');
    """)


def register_parquet_view(con, table_name, parquet_path):
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW {table_name} AS 
        SELECT * FROM read_parquet('{parquet_path}');
    """)


def unregister_table(con, table_name):
    try:
        con.unregister(table_name)
    except Exception:
        pass


def reset_database(con, db_path=':memory:'):
    con.close()
    return duckdb.connect(database=db_path)
