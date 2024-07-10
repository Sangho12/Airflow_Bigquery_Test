import json
from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

# Config variables
BQ_CONN_ID = "af_project"
BQ_PROJECT = "ardent-pact-428803-t9"#.
BQ_DATASET = "bitcoin_cryptocurrency"

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,    
    'start_date': datetime(2023, 12, 1),
    'end_date': datetime(2023, 12, 5),
    'email': ['joe.ho@admazes.com'], 
    'email_on_failure': True,
    'email_on_retry': 5,
    'retries': True,
    'retry_delay': timedelta(seconds=10),
}

# Set Schedule: Run pipeline once a day. 
# Use cron to define exact time. Eg. 8:15am would be "15 08 * * *"
schedule_interval = timedelta(days=1)

# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
    'af-project',  
    default_args=default_args, 
    schedule_interval=schedule_interval
    )

t2_query = f'''
        #standardSQL
        SELECT
          FORMAT_TIMESTAMP("%Y%m%d", block_timestamp) AS Day,
          SUM(size) AS Bitcoin_size
        FROM
          `ardent-pact-428803-t9.bitcoin_cryptocurrency.transactions`
        GROUP BY
          Day
        LIMIT 
          1
        '''

with dag:
  t1 = BigQueryCheckOperator(
        task_id='check',  
        sql='''
        #standardSQL
        SELECT
          *
        FROM
          `ardent-pact-428803-t9.bitcoin_cryptocurrency.transactions`
        limit 10
        ''',
        use_legacy_sql=False,
        gcp_conn_id=BQ_CONN_ID
    ) 

  t2 = BigQueryInsertJobOperator(   #BigQueryOperator
        task_id='Write',    
        configuration={
            "query": {
              "query": t2_query,
              "write_disposition": 'WRITE_APPEND',
              "allow_large_results": True,
              "use_legacy_sql": False,
              "destinationTable": {
                  "projectId": "ardent-pact-428803-t9",
                  "datasetId": "bitcoin_cryptocurrency",
                  "tableId": "bitcoin_insight"
              }
            }
        },
        gcp_conn_id=BQ_CONN_ID
      )
  t1 >> t2



# Setting up Dependencies

