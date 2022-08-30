# Import Libraries
import pandas as pd
import requests
import json
import pyodbc
from UTIL import _config as cfg

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
print(df.head())
# Database connection
server = "BAC00627"
database = "TEST"
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
    			label           nvarchar(50) NULL,
    			adjDividend     DECIMAL(16,10) NULL,
    			dividend        DECIMAL(16,2) NULL,
    			recordDate      DATE NULL,
    			paymentDate     DATE NULL,
    			declarationDate DATE NULL
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
                    INSERT INTO HISTORICAL_DIVIDENDS (date, label, adjDividend, dividend, recordDate, paymentDate, declarationDate)
                    VALUES (NULLIF(?,''),?,?,?,NULLIF(?,''),NULLIF(?,''),NULLIF(?,''))
                    ''',
                    row.date,
                    row.label,
                    row.adjDividend,
                    row.dividend,
                    row.recordDate,
                    row.paymentDate,
                    row.declarationDate
                    )
    conn.commit()
    print("Load data to table successfully")
except ValueError:
    print("Can't load data to database")


