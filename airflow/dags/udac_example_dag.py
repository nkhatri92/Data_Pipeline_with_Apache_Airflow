from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')
aws_hook = AwsHook("aws_credentials")
credentials = aws_hook.get_credentials()

# Set these access key and secret key in these variables

AWS_KEY = credentials.access_key
AWS_SECRET = credentials.secret_key

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 3),
    'email_on_failure': False,
    'email_on_retry': False,    
    'retries': 1,
    'catchup': False,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    
    table_name='staging_events',
    redshift_conn_id='redshift',
    #s3_bucket='udac-stg-bucket',
    s3_bucket='udacity-dend',
    s3_key='log_data/2018/11/{ds}-events.json',
    path = 's3://udacity-dend/log_json_path.json',
    delimiter=',',
    headers='1',
    quote_char='"',
    file_type='json',
    aws_credentials={'key': AWS_KEY,'secret': AWS_SECRET},
    region = 'us-west-2',
    provide_context=True

)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    
    table_name='staging_songs',
    redshift_conn_id='redshift',
    s3_bucket='udacity-dend',
    s3_key="song_data/A/A/A",
    delimiter=',',
    headers='1',
    quote_char='"',
    file_type='json',
    aws_credentials={
    'key': AWS_KEY,
    'secret': AWS_SECRET
    },
    region = 'us-west-2'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stat=SqlQueries.songplay_table_insert,
    target_table="songplays"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stat=SqlQueries.user_table_insert,
    target_table="users",
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stat=SqlQueries.song_table_insert,
    target_table="songs",
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stat=SqlQueries.artist_table_insert,
    target_table="artists",
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stat=SqlQueries.time_table_insert,
    target_table="time",
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stats_tests = [
        (SqlQueries.count_of_nulls_in_songs_table, 0),
        (SqlQueries.count_of_nulls_in_users_table, 0),
        (SqlQueries.count_of_nulls_in_artists_table, 0),
        (SqlQueries.count_of_nulls_in_time_table, 0),
        (SqlQueries.count_of_nulls_in_songplays_table, 0),
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_user_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
