from airflow.sdk import dag, task
from utils.etl import load_customers_scd2, load_fact_orders

@dag(
    dag_id="sales_pipeline_dag",
    schedule="@daily"
)
def sales_pipeline():
    @task
    def extract():
        print('extraction is handled in etl function')

    @task
    def transformation():
        load_customers_scd2()
        print('transformation is Done')

    @task
    def load():
        load_fact_orders()
        print('loading is Done')


    extract() >> transformation() >> load()

sales_pipeline()