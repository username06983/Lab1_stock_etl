This project loads stock price data into Snowflake using Airflow, transforms it with dbt, and visualizes key metrics in a BI dashboard.

Airflow (ETL):
* Loads raw stock data into a Snowflake RAW table.

dbt (ELT):
* Uses the ETL output as input for transformations.
* Creates a final transformed table containing moving average and daily price change.
* dbt models are scheduled and executed from Airflow.

BI Tool
* The dashboard is built using the dbt transformed table.

Files:
Airflow DAGs → dags/
dbt models → lab2/models/
dbt snapshots → lab2/snapshot/
