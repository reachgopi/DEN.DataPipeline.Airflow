from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.data_quality import DataQualityOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.stage_redshift import StageToRedshiftOperator
from helpers.sql_queries import SqlQueries

#Default Args for the Dag
default_args = {
    'owner': 'GopiShan',
    'start_date': datetime(2018, 11, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5), # Retries every 5 mins incase of failures
    'depends_on_past': False, # Flag to indicate if it need to wait for historical runs or parallelize
    'email_on_failure': False, # email failur is turned off
    'catchup':False
}

#Dag Initialization
dag = DAG('airflow_de_project',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Calling StageToRedShift Custom Operator to load data into staging events table
stage_log_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_connection_id='redshift',
    table_name='staging_events',
    aws_credential_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log-data/{execution_date.year}/{execution_date.month}',
    json_path="s3://udacity-dend/log_json_path.json",
    dag=dag
)

# Calling StageToRedShift Custom Operator to load data into songs staging table
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_connection_id='redshift',
    table_name='staging_songs',
    aws_credential_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song-data/A/A',
    json_path="auto",
    dag=dag
)

# Calling LoadFactOperator to load the data into songplays fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    sql_statement=SqlQueries.songplay_table_insert,
    target_table='songplays',
    redshift_connection_id='redshift',
    dag=dag
)

# Calling LoadDimensionOperator to load the data into users dimension table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    target_table='users',
    redshift_connection_id='redshift',
    sql_statement=SqlQueries.user_table_insert,
    truncate=False,
    dag=dag
)

# Calling LoadDimensionOperator to load the data into songs dimension table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    target_table='songs',
    redshift_connection_id='redshift',
    sql_statement=SqlQueries.song_table_insert,
    truncate=False,
    dag=dag
)

# Calling LoadDimensionOperator to load the data into artists dimension table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    target_table='artists',
    redshift_connection_id='redshift',
    sql_statement=SqlQueries.artist_table_insert,
    truncate=False,
    dag=dag
)

# Calling LoadDimensionOperator to load the data into time dimension table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    target_table='time',
    redshift_connection_id='redshift',
    sql_statement=SqlQueries.time_table_insert,
    truncate=False,
    dag=dag
)

# Runs the QualityCheck via DataQualityOperator on all fact and dimension tables once the data is loaded
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_connection_id = 'redshift',
    target_table=("songplays","users","songs","artists","time"),
    validate_column=("playid","userid","song_id","artist_id","start_time"),
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task order
start_operator >> [stage_log_events_to_redshift,stage_songs_to_redshift]
[stage_log_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table]
[load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator