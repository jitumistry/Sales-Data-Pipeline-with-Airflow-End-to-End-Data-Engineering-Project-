from utils.etl import load_customers_scd2, load_fact_orders

load_customers_scd2()
load_fact_orders()

print("ETL completed!")
