from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from operators import CreateStageOperator, StageToRedshiftOperator
# from operators import (StageToRedshiftOperator, LoadFactOperator,
#                        LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries, S3VariableManager, RedshiftVariableManager

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
    redshift_var_mgr = RedshiftVariableManager()

    ###### DAG run starts here ######
    start_operator = EmptyOperator(task_id='Begin_execution')

    # create tables prior to staging
    create_songs_staging_table_queries = sql_queries.drop_songs_staging_table + sql_queries.create_songs_staging_table
    create_events_staging_table_queries = sql_queries.drop_events_staging_table + sql_queries.create_events_staging_table

    create_songs_staging_table = CreateStageOperator(
        task_id='create_songs_staging_table',
        conn_id=conn_id,
        sql=create_songs_staging_table_queries
    )

    create_events_staging_tble = CreateStageOperator(
        task_id='create_events_staging_table',
        conn_id=conn_id,
        sql=create_events_staging_table_queries
    )

    # staging: load data as-is
    s3_bucket_key = 's3_bucket'

    ## get S3 configs from Airflow
    s3_var_manager = S3VariableManager(s3_bucket_key)

    s3_bucket = s3_var_manager.get_bucket_name()
    s3_songs_dir = s3_var_manager.get_dir_prefix('s3_object_song_prefix')
    s3_events_dir = s3_var_manager.get_dir_prefix('s3_object_log_prefix')

    songs_stage_col_mapping_config = redshift_var_mgr.get_mapping_config('staging_song_col_mapping')
    events_stage_col_mapping_config = redshift_var_mgr.get_mapping_config('staging_events_col_mapping')
    
    stg_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs_to_redshift',
        conn_id=conn_id,
        s3_bucket=s3_bucket, 
        s3_dir=s3_songs_dir,
        staging_table='songs_staging',
        include_filter=False,      # get objects from non-'log-data' folder
        staging_col_mapping_config = songs_stage_col_mapping_config
    )

    stg_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events_to_redshift',
        conn_id=conn_id,
        s3_bucket=s3_bucket,
        s3_dir=s3_events_dir,
        staging_table='events_staging',
        include_filter=True,       # get objects from 'log-data' folder
        staging_col_mapping_config = events_stage_col_mapping_config
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
    start_operator >> [
        create_songs_staging_table >> stg_songs_to_redshift,
        create_events_staging_tble >> stg_events_to_redshift
    ]

final_project_dag = final_project()
