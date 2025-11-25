This project loads stock price data into Snowflake using Airflow, transforms it with dbt, and visualizes metrics in a BI dashboard.
Airflow (ETL):
Loads raw stock data into a Snowflake RAW table.
dbt (ELT):
ETL output data is used as ELT input
Creates a transformed table with moving average and daily price change.
This table is scheduled and run from Airflow.
BI Tool:
Dashboard built using the dbt-transformed table.
Files:
Airflow DAGs → dags/
dbt models → lab2/models/
dbt snapshot → lab2/snapshot/
