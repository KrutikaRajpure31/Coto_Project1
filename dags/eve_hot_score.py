from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_mongodb_data():
    try:
        client = MongoClient("mongodb+srv://qa_user:E8b528lsiWSApMam@coto-qa-mongodb.xyujs.mongodb.net/?retryWrites=true&w=majority", 27017, tls=True, tlsAllowInvalidCertificates=True)
        print("Connected successfully!!!")
    except:
        print("Could not connect to MongoDB")
        return None

    pipeline = [
        {
        '$lookup': {
            'from': 'categories',
            'localField': 'category_id',
            'foreignField': 'id',
            'as': 'categories'
        }
    },
    {
        '$unwind': '$categories'
    },
    {
        '$lookup': {
            'from': 'sub_categories',
            'localField': 'category_id',
            'foreignField': 'category_id',
            'as': 'subcategories'
        }
    },
    {
        '$unwind': '$subcategories'
    },
    {
        '$sort': {
            'hotness_score': -1
        }
    },
    {
        '$project': {
            'content_id':1,
            'category_id':1,
            'Category_title':'$categories.title',
            'content_type':1,
            'total_likes': 1,
            'total_comments':1,
            'total_shares':1,
            'moderation_status':1,
            'trending_score':1,
            'Content_popularity_score':1,
            'hotness_score':1,
            'Category_popularity_score':'$categories.popularity_score' ,
            'SubCategory_title': '$subcategories.title',
            'subcategories_popularity_score':'$subcategories_popularity_score'
        }
    }
    ]
    
    eve_hot_score = list(client['media_data']['contents'].aggregate(pipeline))
    eve_hot_score = pd.DataFrame(eve_hot_score)
    return eve_hot_score

with DAG('eve_hot_score', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    fetch_task = PythonOperator(
        task_id='fetch_mongodb_data_task',
        python_callable=fetch_mongodb_data,
    )

if __name__ == "__main__":
    dag.cli()
