
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'feature_user_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
)

def feature_user_data():
    # Your provided Python code here
    from pymongo import MongoClient
    import pandas as pd
    client = MongoClient("mongodb+srv://qa_user:E8b528lsiWSApMam@coto-qa-mongodb.xyujs.mongodb.net/?retryWrites=true&w=majority", 27017, tls=True, tlsAllowInvalidCertificates=True)
    pipeline=[
    {
        '$lookup': {
            'from': 'users',
            'localField': 'user_id',
            'foreignField': 'user_id',
            'as': 'Users'
        }
    },
    {
        '$unwind': '$Users'
    },
    {
        '$lookup': {
            'from': 'sub_categories',
            'localField': 'subcategory_id',
            'foreignField': 'id',
            'as': 'subcategories'
        }
    },
    {
        '$unwind': '$subcategories'
    },
    {
        '$lookup': {
            'from': 'categories',
            'localField': 'subcategories.category_id',
            'foreignField': 'id',
            'as': 'categories'
        }
    },
    {
        '$unwind': '$categories'
    },
    {
        '$sort': {
            'engager_score': -1
        }
    },
    {
        '$project': {
            'normalized_score': 1,
            'user_id': 1,
            'User name': '$Users.user_name',
            'User Address':'$Users.address_line1',
            'subcategory_id': 1,
            'start_date': 1,
            'end_date': 1,
            'engager_score': 1, 
            'subcategories_title':'$subcategories.title', 
            'categories_title':'$categories.title',
            'subcategories_popularity_score':'$subcategories_popularity_score',
            'categories_popularity_score':'$categories_popularity_score'
        }
    }
    ]
    feature_user = list(client['media_data']['30_days_featured_user_score'].aggregate(pipeline))
    feature_user = pd.DataFrame(feature_user)
    feature_user.to_csv('feature_user.csv')

# Define tasks
task_process_media_data = PythonOperator(
    task_id='feature_user_data',
    python_callable=feature_user_data,
    dag=dag,
)

# Set task dependencies
task_process_media_data

if __name__ == "__main__":
    dag.cli()
