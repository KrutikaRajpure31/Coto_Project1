from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import pandas as pd

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'community_engagement_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
)

def connect_mongodb_and_process():
    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['media_data']

    # Your provided code here
    pipeline = [
        {
        '$lookup': {
            'from': 'sub_categories',
            'localField': 'subcategory_id',
            'foreignField': 'id',
            'as': 'subcategory'
        }
    },
    {
        '$unwind': '$subcategory'
    },
    {
        '$lookup': {
            'from': 'communities',
            'localField': 'community_id',
            'foreignField': 'community_id',
            'as': 'community'
        }
    },
    {
        '$unwind': '$community'
    },
    {
        '$sort': {
            'community_engagement_score': -1
        }
    },
    {
        '$project': {
            'community_id': 1,
            'subcategory_id':1,
            'start_date': 1,
            'end_date': 1,
            'days_ago':1,
            'community_engagement_score':1,
            'Subcategory_title': '$subcategory.title',
            'Subcategory_popularity_score':'$subcategory.popularity_score',
            'Category_ID': '$subcategory.category_id',
            'Community_title':'$community.title',
            'Community_sub_title':'$community.sub_title',
            'total_active_posts':'$community.total_active_posts'
        }
    }
    ]
    community_engagement_contentteam = list(db['community_engagement_score'].aggregate(pipeline))
    community_engagement_contentteam = pd.DataFrame(community_engagement_contentteam)

    # Save the processed data to a file or perform further actions
    community_engagement_contentteam.to_csv('community_engagement_data.csv')

# Define tasks
task_connect_and_process = PythonOperator(
    task_id='connect_and_process',
    python_callable=connect_mongodb_and_process,
    dag=dag,
)

# Set task dependencies
task_connect_and_process

if __name__ == "__main__":
    dag.cli()
