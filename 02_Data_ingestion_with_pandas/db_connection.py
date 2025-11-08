from dotenv import load_dotenv, find_dotenv
import os
# import mysql.connector
from sqlalchemy import create_engine, text
import pandas as pd

env_path = find_dotenv() # Looks for .env file containing mysql credentials.
if not env_path:
    raise FileNotFoundError("Could not find a .env file")
load_dotenv(env_path)

def get_engine(db_name: str):
    """Creates a reusable SQLALchemy engine."""
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    host = os.getenv("MYSQL_HOST", "127.0.0.1")
    port = os.getenv("MYSQL_PORT", "3306")
    engine_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"
    return create_engine(engine_url, echo=False, pool_pre_ping=True)

# --- Helper functions ---
def run_query(db_name: str, sql: str, params=None):
    """Run query and return list of tuples."""
    engine = get_engine(db_name)
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        return result.fetchall()

def fetch_dataframe(db_name: str, sql: str, params=None):
    """Run query and return pandas DataFrame."""
    engine = get_engine(db_name)
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)

# --- Quick test ---
if __name__ == "__main__":
    try:
        df = fetch_dataframe("albums", "SHOW TABLES;")
        print(df)
        print("✅ Connection successful using SQLAlchemy.")
    except Exception as e:
        print("❌ Connection failed:", e)








#load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))


# def connect_database(db_name: str):
#     """Create and return connection to a database."""
#     connection = mysql.connector.connect(
#         host=os.getenv("MYSQL_HOST"),
#         user=os.getenv("MYSQL_USER"),
#         password=os.getenv("MYSQL_PASSWORD"),
#         database=db_name,
#         port=int(os.getenv("MYSQL_PORT"))
#     )
#     return connection

# def run_query(db_name: str, sql: str, params=None):
#     """Runs query and return the results as tuples."""
#     with connect_database(db_name) as conn:
#         with conn.cursor() as cur:
#             cur.execute(sql, params or ())
#             return cur.fetchall()
        

# def fetch_dataframe(db_name: str, sql: str, params=None):
#     """Runs a query and return results as a pandas DataFrame."""
#     with connect_database(db_name) as conn:
#         return pd.read_sql(sql, conn, params=params)
    

# # Optional quick test
# if __name__ == "__main__":
#     try:
#         rows = run_query("albums", "SHOW TABLES;")
#         print(rows)
#         print("✅ Connection successful.")
#     except Exception as e:
#         print("❌ Connection failed:", e)


# if __name__ == "__main__":
#     try:
#         conn = connect_database("albums")
#         cursor = conn.cursor()
#         cursor.execute("SHOW TABLES;")
#         for table in cursor.fetchall():
#             print(table)
#         cursor.close()
#         conn.close()
#         print("✅ Connection successful.")
#     except Exception as e:
#         print("❌ Connection failed:", e)
