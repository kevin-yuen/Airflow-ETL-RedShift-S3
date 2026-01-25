from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from operators import StageOperator
# from operators import (StageToRedshiftOperator, LoadFactOperator,
#                        LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

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
    sql_queries = SqlQueries()

    ###### DAG run starts here ######
    start_operator = EmptyOperator(task_id='Begin_execution')

    # create tables prior to staging
    create_songs_staging_table_queries = sql_queries.drop_songs_staging_table + sql_queries.create_songs_staging_table
    create_events_staging_table_queries = sql_queries.drop_events_staging_table + sql_queries.create_events_staging_table
    create_staging_tables_queries = create_songs_staging_table_queries + create_events_staging_table_queries

    create_staging_tables = StageOperator(
        task_id='create_staging_tables',
        conn_id='redshift',
        sql=create_staging_tables_queries
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
    start_operator >> create_staging_tables

final_project_dag = final_project()
