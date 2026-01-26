from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from operators import StageOperator, StageToRedshiftOperator
# from operators import (StageToRedshiftOperator, LoadFactOperator,
#                        LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries, S3VariableManager

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2026, 1, 1, tz='UTC'),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

@dag(
    dag_id='airflow-etl-redshift-s3-data-pipeline',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule='@hourly'      # equivalent to '0 * * * *'
)
def final_project():
    conn_id = 'redshift'
    sql_queries = SqlQueries()

    ###### DAG run starts here ######
    start_operator = EmptyOperator(task_id='Begin_execution')

    # create tables prior to staging
    create_songs_staging_table_queries = sql_queries.drop_songs_staging_table + sql_queries.create_songs_staging_table
    create_events_staging_table_queries = sql_queries.drop_events_staging_table + sql_queries.create_events_staging_table
    create_staging_tables_queries = create_songs_staging_table_queries + create_events_staging_table_queries

    create_staging_tables = StageOperator(
        task_id='create_staging_tables',
        conn_id=conn_id,
        sql=create_staging_tables_queries
    )

    # staging: load data as-is
    s3_bucket_key = 's3_bucket'

    ## get S3 configs from Airflow
    s3_var_manager = S3VariableManager(s3_bucket_key)

    s3_bucket = s3_var_manager.get_bucket_name()
    s3_song_dir = s3_var_manager.get_dir_prefix('s3_object_song_prefix')
    s3_events_dir = s3_var_manager.get_dir_prefix('s3_object_log_prefix')

    stg_table_s3_folder_mapping = {
        'songs_staging': s3_song_dir,
        'events_staging': s3_events_dir
    }
    
    stg_to_redshift_op = StageToRedshiftOperator(
        task_id='stage_to_redshift',
        conn_id=conn_id,
        s3_bucket=s3_bucket, 
        stg_s3_folder_mapping=stg_table_s3_folder_mapping
    )

    # stage_events_to_redshift = StageToRedshiftOperator(
    #     task_id='Stage_events',
    # )

    # stage_songs_to_redshift = StageToRedshiftOperator(
    #     task_id='Stage_songs',
    # )

    # load_songplays_table = LoadFactOperator(
    #     task_id='Load_songplays_fact_table',
    # )

    # load_user_dimension_table = LoadDimensionOperator(
    #     task_id='Load_user_dim_table',
    # )

    # load_song_dimension_table = LoadDimensionOperator(
    #     task_id='Load_song_dim_table',
    # )

    # load_artist_dimension_table = LoadDimensionOperator(
    #     task_id='Load_artist_dim_table',
    # )

    # load_time_dimension_table = LoadDimensionOperator(
    #     task_id='Load_time_dim_table',
    # )

    # run_quality_checks = DataQualityOperator(
    #     task_id='Run_data_quality_checks',
    # )

    # pipelines
    # test_task = test()

    # dependencies
    start_operator >> create_staging_tables >> stg_to_redshift_op

final_project_dag = final_project()
