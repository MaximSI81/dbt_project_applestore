from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
import pendulum

# ДОЛЖНО БЫТЬ 3 РАЗНЫХ CONNECTION_ID!
PRODUCTS_CONNECTION_ID = 'b5785a9b-fee9-4912-92d4-3e4b4abb06e5'
SALES_CONNECTION_ID = 'a4df1c6f-fae5-4cab-9635-ec16f571da7e'  
CUSTOMERS_CONNECTION_ID = '1b6dc6c4-4f7d-4644-bfc5-f3fdfda6540a'

with DAG(dag_id='apple_sales_pipeline',
        default_args={'owner': 'airflow'},
        schedule=None,
        start_date=pendulum.today('UTC').add(days=-1),
        catchup=False
   ) as dag:

    # 1. Синхронизация продуктов
    trigger_products_sync = AirbyteTriggerSyncOperator(
        task_id='trigger_products_sync',
        airbyte_conn_id='airbyte_s3_to_pg',
        connection_id=PRODUCTS_CONNECTION_ID,
        asynchronous=True
    )
    
    wait_for_products_sync = AirbyteJobSensor(
        task_id='wait_for_products_sync',
        airbyte_conn_id='airbyte_s3_to_pg',
        airbyte_job_id=trigger_products_sync.output,
        timeout=3600
    )

    # 2. Синхронизация продаж
    trigger_sales_sync = AirbyteTriggerSyncOperator(
        task_id='trigger_sales_sync',
        airbyte_conn_id='airbyte_s3_to_pg',
        connection_id=SALES_CONNECTION_ID,
        asynchronous=True
    )
    
    wait_for_sales_sync = AirbyteJobSensor(
        task_id='wait_for_sales_sync',
        airbyte_conn_id='airbyte_s3_to_pg',
        airbyte_job_id=trigger_sales_sync.output,
        timeout=3600
    )

    # 3. Синхронизация покупателей
    trigger_customers_sync = AirbyteTriggerSyncOperator(
        task_id='trigger_customers_sync',
        airbyte_conn_id='airbyte_s3_to_pg',
        connection_id=CUSTOMERS_CONNECTION_ID,
        asynchronous=True
    )
    
    wait_for_customers_sync = AirbyteJobSensor(
        task_id='wait_for_customers_sync',
        airbyte_conn_id='airbyte_s3_to_pg',
        airbyte_job_id=trigger_customers_sync.output,
        timeout=3600
    )

    # Определение зависимостей
    trigger_products_sync >> wait_for_products_sync
    trigger_sales_sync >> wait_for_sales_sync  
    trigger_customers_sync >> wait_for_customers_sync