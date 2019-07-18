from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

"""
DAG notes:  
The dag is scheduled start immediately to run every hour.
If there's failure in the workflow/process the dag will retry 3 times every 5 mins
Table name would need to be provided so the process would know what table to load.
File type must be provided so the process knows to load a json file or csv file. i.e file_type ='json' or  file_type = 'csv'
If the file is a json file then add the additonal parameter: json="auto" or json=/filename.json (in case copy need to map the fields correctly to the table)

"""

default_args = {
    'owner': 'udacity',
	'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'email_on_failure': False,
    'email_on_retry': False,
	'retries': 2,
	'retry_delay': timedelta(minutes=5),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',         
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    file_type="json",
    task_id='Stage_events',
    dag=dag,
    table_src="staging_events",
    table_trunc="staging_events",
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    json="/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    file_type="json",
    task_id='Stage_songs',
    dag=dag,
    table_src="staging_songs",
    table_trunc="staging_songs",
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    json="auto"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',  
    query_nm=SqlQueries.songplay_table_insert    
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',  
    query_nm=SqlQueries.user_table_insert
)


load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift', 
    query_nm=SqlQueries.song_table_insert  
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',       
    query_nm=SqlQueries.artist_table_insert    
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',  
    query_nm=SqlQueries.time_table_insert   
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',  
    query_nm=SqlQueries.data_check_qry    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Task ordering for the DAG tasks 
#

start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift  >> load_songplays_table
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator