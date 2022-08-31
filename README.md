# dime-financial-api
This project is provide for qualification process at Dime.

## Purpose
Ingest data from a free financial data API by using HTTP request. Afterward, load the data to local database.

## Environments
* Executor: Python 3.9
* Database: MSSQL server 2019 (version 15.0.2000.5)

  ### Python packages
  * pyodbc 4.0.31
  * pandas  1.3.4
  * request 2.26.0
  * json 2.0.9

## How to use
1. Modify _config_template.py before use
``` Ruby
api_key = 'xxx'
db_server = 'xxx'
db_name = 'xxx'
db_user = 'xxx'
db_pass = 'xxx'
```
2. Create batch file to execute the python file by following template
``` Ruby
"DRIVE:\YOUR PYTHON INTERPRETER PATH" "DRIVE:\PATH\delisted_companies.py"
"DRIVE:\YOUR PYTHON INTERPRETER PATH" "DRIVE:\PATH\dime-financial-api\delisted_companies.py"
```
