# Import Libraries
import pandas as pd
import requests
import json
import pyodbc
from UTIL import _config as cfg
from datetime import datetime

# HTTP Request
api_key = cfg.api_key
url = f"https://financialmodelingprep.com/api/v3/historical-price-full/stock_dividend/AAPL?apikey={api_key}"
try:
    r = requests.get(url)
    data = json.loads(r.text)
    print("Request successful", r)
except:
    print("Failed to request", r)

# Create Dataframe
df = pd.DataFrame()
for i in data['historical']:
    df = df.append(i,ignore_index=True)
print("Rows:", len(df))

# Add ETL Date
df["ETL_Date"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Database connection
server = cfg.db_server
database = "financedb"
username = cfg.db_user
password = cfg.db_pass
try:
    conn = pyodbc.connect('DRIVER={SQL Server};'
                          f'SERVER={server};'
                          f'DATABASE={database};'
                          f'UID={username};'
                          f'PWD={password};'
                          'Trusted_Connection=no'
                    )
    cursor = conn.cursor()
    print("Successfully connect to database")
except ConnectionError:
    print("Failed to connect to database")

# Create table
try:
    cursor.execute('''
            IF NOT EXISTS (SELECT * FROM sysobjects where name='HISTORICAL_DIVIDENDS')
    		CREATE TABLE HISTORICAL_DIVIDENDS (
    			date            DATE NULL,
    			label           NVARCHAR(MAX) NULL,
    			adjDividend     DECIMAL(16,10) NULL,
    			dividend        DECIMAL(16,2) NULL,
    			recordDate      DATE NULL,
    			paymentDate     DATE NULL,
    			declarationDate DATE NULL,
    			ETL_Date        DATETIME
    			)
                   ''')
    conn.commit()
    print("Create table completed successfully")
except ValueError:
    print("Can't create table")

# Truncate table
try:
    cursor.execute(''' TRUNCATE TABLE HISTORICAL_DIVIDENDS''')
    print("Truncate table completed successfully")
except ValueError:
    print("Can't truncate table")

# Full load data
try:
    for row in df.itertuples():
        cursor.execute('''
                    INSERT INTO HISTORICAL_DIVIDENDS (date, label, adjDividend, dividend, recordDate, paymentDate, declarationDate, ETL_Date)
                    VALUES (NULLIF(?,''), ?, ?, ?, NULLIF(?,''), NULLIF(?,''), NULLIF(?,''), ?)
                    ''',
                    row.date,
                    row.label,
                    row.adjDividend,
                    row.dividend,
                    row.recordDate,
                    row.paymentDate,
                    row.declarationDate,
                    row.ETL_Date
                    )
    conn.commit()
    print("Load data to table successfully")
except ValueError:
    print("Can't load data to database")


