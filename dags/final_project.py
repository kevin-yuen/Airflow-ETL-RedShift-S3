from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.providers.standard.operators.empty import EmptyOperator
from helpers import SqlQueries, S3VariableManager, RedshiftVariableManager
from operators import (
    CreateStageOperator,
    StageToRedshiftOperator,
    StagingDataQualityOperator,
    CreateFactOperator,
    LoadFactOperator,
    FactDataQualityOperator,
    CreateDimensionOperator,
    LoadDimensionOperator,
    DimDataQualityOperator
)


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2026, 1, 1, tz='UTC'),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

@dag(
    dag_id='airflow-etl-redshift-s3-data-pipeline',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule='@hourly',      # equivalent to '0 * * * *'
    params={
        'drop_table': False
    }
)
def final_project():
    conn_id = 'redshift'
    sql_queries = SqlQueries()
    redshift_var_mgr = RedshiftVariableManager()

    ###### DAG run starts here ######
    start_operator = EmptyOperator(task_id='Begin_execution')

    # create staging tables (if not exists)
    songs_stage_ds_name = redshift_var_mgr.get_ds_name('staging_song_ds_name')
    events_stage_ds_name = redshift_var_mgr.get_ds_name('staging_events_ds_name')

    create_songs_staging_table = CreateStageOperator(
        task_id='create_songs_staging_table',
        conn_id=conn_id,
        sql=sql_queries.create_songs_staging_table,
        parameters={
            'dataset_name': songs_stage_ds_name
        }
    )

    create_events_staging_tble = CreateStageOperator(
        task_id='create_events_staging_table',
        conn_id=conn_id,
        sql=sql_queries.create_events_staging_table,
        parameters={
            'dataset_name': events_stage_ds_name
        }
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
        staging_table=songs_stage_ds_name,
        include_filter=False,      # get objects from non-'log-data' folder
        staging_col_mapping_config = songs_stage_col_mapping_config,
    )

    stg_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events_to_redshift',
        conn_id=conn_id,
        s3_bucket=s3_bucket,
        s3_dir=s3_events_dir,
        staging_table=events_stage_ds_name,
        include_filter=True,       # get objects from 'log-data' folder
        staging_col_mapping_config = events_stage_col_mapping_config,
    )

    # post-staging data quality check
    stg_songs_dq_check = StagingDataQualityOperator(
        task_id='stage_songs_dq_check',
        conn_id=conn_id,
        dq_type=['row_count_check', 'null_count_check', 'uniqueness_count_check'],
        ds_name=songs_stage_ds_name,
        ds_columns=['song_id', 'artist_id']
    )

    stg_events_dq_check = StagingDataQualityOperator(
        task_id='stage_events_dq_check',
        conn_id=conn_id,
        dq_type=['row_count_check', 'null_count_check'],
        ds_name=events_stage_ds_name,
        ds_columns=['sessionid', 'ts']
    )

    # data loading: fact table
    songplays_fact_ds_name = redshift_var_mgr.get_ds_name('fact_songplays_ds_name')

    create_songplays_fact_table = CreateFactOperator(
        task_id='create_songplays_fact_table',
        conn_id=conn_id,
        sql=sql_queries.create_songplays_fact_table,
        parameters={
            'dataset_name': songplays_fact_ds_name
        }
    )

    load_songplays_fact_table = LoadFactOperator(
        task_id='load_songplays_fact_table',
        conn_id=conn_id,
        ds_name=songplays_fact_ds_name,
        sql=sql_queries.songplay_table_insert
    )

    # data quality check on fact table
    fact_dq_check = FactDataQualityOperator(
        task_id='fact_dq_check',
        conn_id=conn_id,
        dq_type=['reasonableness_check', 'uniqueness_cnt_check_fact', 'non_null_check'],
        ds_name=songplays_fact_ds_name,
        ds_columns=['songplay_id', 'start_time', 'sessionid', 'userid']
    )

    # data loading: dim tables    
    artists_dim_ds_name = redshift_var_mgr.get_ds_name('dim_artists_ds_name')
    songs_dim_ds_name = redshift_var_mgr.get_ds_name('dim_songs_ds_name')
    time_dim_ds_name = redshift_var_mgr.get_ds_name('dim_time_ds_name')
    users_dim_ds_name = redshift_var_mgr.get_ds_name('dim_users_ds_name')

    load_mode = redshift_var_mgr.get_dim_load_mode('dim_load_mode')

    create_artists_dim_table = CreateDimensionOperator(
        task_id='create_artists_dim_table',
        conn_id=conn_id,
        sql=sql_queries.create_artists_dim_table,
        parameters={
            'dataset_name': artists_dim_ds_name,
            'mode': load_mode
        }
    )

    load_artists_dim_table = LoadDimensionOperator(
        task_id='load_artists_dim_table',
        conn_id=conn_id,
        ds_name=artists_dim_ds_name,
        sql=sql_queries.artist_table_insert
    )

    create_songs_dim_table = CreateDimensionOperator(
        task_id='create_songs_dim_table',
        conn_id=conn_id,
        sql=sql_queries.create_songs_dim_table,
        parameters={
            'dataset_name': songs_dim_ds_name,
            'mode': load_mode
        }
    )

    load_songs_dim_table = LoadDimensionOperator(
        task_id='load_songs_dim_table',
        conn_id=conn_id,
        ds_name=songs_dim_ds_name,
        sql=sql_queries.song_table_insert
    )

    create_time_dim_table = CreateDimensionOperator(
        task_id='create_time_dim_table',
        conn_id=conn_id,
        sql=sql_queries.create_time_dim_table,
        parameters={
            'dataset_name': time_dim_ds_name,
            'mode': load_mode
        }
    )

    load_time_dim_table = LoadDimensionOperator(
        task_id='load_time_dim_table',
        conn_id=conn_id,
        ds_name=time_dim_ds_name,
        sql=sql_queries.time_table_insert
    )

    create_users_dim_table = CreateDimensionOperator(
        task_id='create_users_dim_table',
        conn_id=conn_id,
        sql=sql_queries.create_users_dim_table,
        parameters={
            'dataset_name': users_dim_ds_name,
            'mode': load_mode
        }
    )

    load_users_dim_table = LoadDimensionOperator(
        task_id='load_users_dim_table',
        conn_id=conn_id,
        ds_name=users_dim_ds_name,
        sql=sql_queries.user_table_insert
    )

    # data quality check on dimension tables
    dq_types = [
        'dim_row_count_check',
        'dim_null_pk_check',
        'dim_pk_uniqueness_check',
        'dim_fk_integrity_check'
    ]

    dq_config = {
        'artists_dim': {
            'pk': 'artistid',
            'dq_types': dq_types
        },
        'songs_dim': {
            'pk': 'songid',
            'dq_types': dq_types
        },
        'users_dim': {
            'pk': 'userid',
            'dq_types': dq_types
        },
        'time_dim': {
            'pk': 'start_time',
            'dq_types': dq_types
        },
    }

    run_data_quality_checks = DimDataQualityOperator(
        task_id='run_data_quality_checks',
        conn_id=conn_id,
        dq_config=dq_config
    )

    ###### DAG run ends here ######
    end_operator = EmptyOperator(task_id='End_execution')

    # dependencies
    start_operator >> [
        create_songs_staging_table >> stg_songs_to_redshift >> stg_songs_dq_check,
        create_events_staging_tble >> stg_events_to_redshift >> stg_events_dq_check
    ] >> create_songplays_fact_table >> \
        load_songplays_fact_table >> \
        fact_dq_check >> [
            create_artists_dim_table >> load_artists_dim_table,
            create_songs_dim_table >> load_songs_dim_table,
            create_time_dim_table >> load_time_dim_table,
            create_users_dim_table >> load_users_dim_table
        ] >> run_data_quality_checks >> end_operator

final_project_dag = final_project()
