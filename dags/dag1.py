from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from pymongo import MongoClient

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 27),
    'retries': 1,
}

def connect_mongodb():
    client = MongoClient("mongodb+srv://qa_user:E8b528lsiWSApMam@coto-qa-mongodb.xyujs.mongodb.net/?retryWrites=true&w=majority", 27017, tls=True, tlsAllowInvalidCertificates=True)
    database = client['media_data']
    return database

def execute_pipeline1(**kwargs):
    client = connect_mongodb()
    pipeline1 = [
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
    eve_hot_score = list(client['media_data']['contents'].aggregate(pipeline1))
    kwargs['ti'].xcom_push(key='pipeline1_result', value=eve_hot_score)

def execute_pipeline2(**kwargs):
    client = connect_mongodb()
    pipeline2 = [
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
            'from': 'categories',
            'localField': 'subcategory.category_id',
            'foreignField': 'id',
            'as': 'category'
        }
    },
    {
        '$unwind': '$category'
    },
    {
        '$sort': {
            'engager_score': -1
        }
    },
    {
        '$project': {
            '_id': 1,
            'normalized_score':1,
            'Subcategory_id': 1,
            'engager_score': 1,
            'Category_id': '$category.category_id',
            'Subcategory_title': '$subcategory.title',
            'Subcategory_popularity_score':'$subcategory.popularity_score',
            'Category_title': '$category.title',
            'Category_popularity_score':'$category.popularity_score',
        }
    }
        ]  # Your pipeline2 definition
    engager_score_content_team = list(client['media_data']['30_days_engager_score'].aggregate(pipeline2))
    kwargs['ti'].xcom_push(key='pipeline2_result', value=engager_score_content_team)


def process_data(**kwargs):
    ti = kwargs['ti']
    eve_hot_score_df = ti.xcom_pull(task_ids='execute_pipeline1')
    engager_score_content_team_df = ti.xcom_pull(task_ids='execute_pipeline2')

    grouped_data = eve_hot_score_df.groupby(['Category_title', 'content_type']).agg({
        'total_likes': 'sum',
        'total_comments': 'sum',
        'total_shares': 'sum'
    }).reset_index()
    
    # Example: Print the grouped data
    print(grouped_data.head())
    print(engager_score_content_team_df.head())

dag = DAG('dag1', default_args=default_args, schedule_interval=None)

connect_task = PythonOperator(
    task_id='connect_mongodb',
    python_callable=connect_mongodb,
    provide_context=True,
    dag=dag,
)

pipeline1_task = PythonOperator(
    task_id='execute_pipeline1',
    python_callable=execute_pipeline1,
    provide_context=True,
    dag=dag,
)

pipeline2_task = PythonOperator(
    task_id='execute_pipeline2',
    python_callable=execute_pipeline2,
    provide_context=True,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
connect_task >> pipeline1_task >> process_data_task
connect_task >> pipeline2_task >> process_data_task