import json
from airflow.decorators import dag, task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from pendulum import datetime


@dag(
    dag_id="load_data_from_mongodb",
    schedule=None,
    start_date=datetime(2023, 8, 27),
    catchup=False,
    default_args={
        "retries": 0,
    },
)
def load_data_to_mongodb():
    t1 = PythonOperator(
    task_id='load_data_to_mongodb',
    python_callable=load_data_to_mongodb,
    provide_context=True,
    dag=dag,
        
    )

    @task
    def uploadtomongo(result):
        hook = MongoHook(mongo_conn_id="mongo_default")
        client = hook.get_conn()
        db = (
            client.media_data
        )  # Replace "MyDB" if you want to load data to a different database
        currency_collection = db.contents
        print(currency_collection.find({}).limit(3))
        print(f"Connected to MongoDB - {client.server_info()}")
        #d = json.loads(result)
        #currency_collection.insert_one(d)

    t1 >> uploadtomongo(result=t1.output)


load_data_to_mongodb()