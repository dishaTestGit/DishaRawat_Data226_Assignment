from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests
def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def return_last_90d_price(symbol):
  vantage_api_key = Variable.get('vantage_api_key')
  url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'
  r = requests.get(url)
  data = r.json()
  results = []
  for d in data["Time Series (Daily)"]:
    stock_info = data["Time Series (Daily)"][d]
    stock_info['date'] = d
    results.append(stock_info)

  return results

@task
def load_data(con, records):
    target_table = "USER_DB_OTTER.RAW.STOCK_PRICE"
    try:
        con.execute("BEGIN;")
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
              OPEN NUMBER,
              HIGH NUMBER,
              LOW NUMBER,
              CLOSE NUMBER,
              TRADE_VOLUME NUMBER,
              TRADE_DATE DATE,
              SYMBOL VARCHAR DEFAULT 'ORCL',
              PRIMARY KEY (TRADE_DATE, SYMBOL)
            );
        """)

        con.execute(f"DELETE FROM {target_table}")

        for r in records:
            open_ = r["1. open"]
            high = r["2. high"]
            low = r["3. low"]
            close = r["4. close"]
            trade_volume = r["5. volume"]
            trade_date = r["date"]

            print("Inserting record...")
            sql = f"""
                INSERT INTO {target_table} (OPEN, HIGH, LOW, CLOSE, TRADE_VOLUME, TRADE_DATE)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            con.execute(sql, (open_, high, low, close, trade_volume, trade_date))

        con.execute("COMMIT;")

        result = con.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()
        print(f"Total rows inserted: {result[0]}")

    except Exception as e:
        con.execute("ROLLBACK;")
        print("Error:", e)
        raise e
    
with DAG(
    dag_id = 'Assignement5',
    start_date = datetime(2025,10,2),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
      
    cur = return_snowflake_conn()
    price_list = return_last_90d_price("ORCL")
    load_task = load_data(cur,price_list)
    price_list >> load_task