# Import Libraries
import pandas as pd
import requests
import json
import pyodbc
from UTIL import _config as cfg
from datetime import datetime

# HTTP Request
api_key = cfg.api_key
url = f"https://financialmodelingprep.com/api/v3/delisted-companies?page=0&apikey={api_key}"
try:
    r = requests.get(url)
    data = json.loads(r.text)
    print("Request successful", r)
except:
    print("Failed to request", r)

# Create Dataframe
df = pd.DataFrame()
for i in data:
    df = df.append(i,ignore_index=True)
print("Rows:", len(df))

# Add ETL Date
df["ETL_Date"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Database connection
server = cfg.db_server
database = cfg.db_name
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
            IF NOT EXISTS (SELECT * FROM sysobjects where name='DELISTED_COMPANIES')
    		CREATE TABLE DELISTED_COMPANIES (
    			symbol            NVARCHAR(MAX) NULL,
    			companyName       NVARCHAR(MAX) NULL,
    			exchange          NVARCHAR(MAX) NULL,
    			ipoDate           DATE NULL,
    			delistedDate      DATE NULL,
    			ETL_Date          DATETIME
    			)
                   ''')
    conn.commit()
    print("Create table completed successfully")
except ValueError:
    print("Can't create table")

# Truncate table
try:
    cursor.execute(''' TRUNCATE TABLE DELISTED_COMPANIES''')
    print("Truncate table completed successfully")
except ValueError:
    print("Can't truncate table")

# Full load data
try:
    for row in df.itertuples():
        cursor.execute('''
                    INSERT INTO DELISTED_COMPANIES (symbol, companyName, exchange, ipoDate, delistedDate, ETL_Date)
                    VALUES (?, ?, ?, NULLIF(?,''), NULLIF(?,''), ?)
                    ''',
                    row.symbol,
                    row.companyName,
                    row.exchange,
                    row.ipoDate,
                    row.delistedDate,
                    row.ETL_Date
                    )
    conn.commit()
    print("Load data to table successfully")
except ValueError:
    print("Can't load data to database")


