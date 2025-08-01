import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator, BigQueryUpdateTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
from steam_etl import extract_and_upload

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'steam_scd_pipeline',
    default_args=default_args,
    description='ETL pipeline for Steam data with SCD Type 2',
    schedule_interval='@daily',
    start_date=datetime(2025, 7, 19),
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

    create_staging_table = BigQueryCreateEmptyTableOperator(
        task_id='create_staging_table',
        dataset_id='steam_data',
        table_id='cleaned_steam_games_staging',
        project_id='stellar-river-464405-k3',
        schema_fields=[
            {'name': 'appid', 'type': 'INTEGER'},
            {'name': 'name', 'type': 'STRING'},
            {'name': 'genre', 'type': 'STRING', 'mode': 'REPEATED'},
            {'name': 'tags', 'type': 'STRING', 'mode': 'REPEATED'},
            {'name': 'positive', 'type': 'INTEGER'},
            {'name': 'negative', 'type': 'INTEGER'},
            {'name': 'developer', 'type': 'STRING'},
            {'name': 'publisher', 'type': 'STRING'},
            {'name': 'score_rank', 'type': 'STRING'},
            {'name': 'owners', 'type': 'STRING'},
            {'name': 'min_owners', 'type': 'INTEGER'},
            {'name': 'max_owners', 'type': 'INTEGER'},
            {'name': 'average_forever', 'type': 'INTEGER'},
            {'name': 'average_2weeks', 'type': 'INTEGER'},
            {'name': 'median_forever', 'type': 'INTEGER'},
            {'name': 'median_2weeks', 'type': 'INTEGER'},
            {'name': 'ccu', 'type': 'INTEGER'},
            {'name': 'price', 'type': 'FLOAT64'},
            {'name': 'initialprice', 'type': 'FLOAT64'},
            {'name': 'discount', 'type': 'FLOAT64'},
            {'name': 'languages', 'type': 'STRING', 'mode': 'REPEATED'},
            {'name': 'load_date', 'type': 'DATE'}
        ],
        gcp_conn_id='google_cloud_default',
    )

    create_cleaned_table = BigQueryCreateEmptyTableOperator(
        task_id='create_cleaned_table',
        dataset_id='steam_data',
        table_id='cleaned_steam_games',
        project_id='stellar-river-464405-k3',
        schema_fields=[
            {'name': 'appid', 'type': 'INTEGER'},
            {'name': 'name', 'type': 'STRING'},
            {'name': 'genre', 'type': 'STRING', 'mode': 'REPEATED'},
            {'name': 'tags', 'type': 'STRING', 'mode': 'REPEATED'},
            {'name': 'positive', 'type': 'INTEGER'},
            {'name': 'negative', 'type': 'INTEGER'},
            {'name': 'developer', 'type': 'STRING'},
            {'name': 'publisher', 'type': 'STRING'},
            {'name': 'score_rank', 'type': 'STRING'},
            {'name': 'owners', 'type': 'STRING'},
            {'name': 'min_owners', 'type': 'INTEGER'},
            {'name': 'max_owners', 'type': 'INTEGER'},
            {'name': 'average_forever', 'type': 'INTEGER'},
            {'name': 'average_2weeks', 'type': 'INTEGER'},
            {'name': 'median_forever', 'type': 'INTEGER'},
            {'name': 'median_2weeks', 'type': 'INTEGER'},
            {'name': 'ccu', 'type': 'INTEGER'},
            {'name': 'price', 'type': 'FLOAT64'},
            {'name': 'initialprice', 'type': 'FLOAT64'},
            {'name': 'discount', 'type': 'FLOAT64'},
            {'name': 'languages', 'type': 'STRING', 'mode': 'REPEATED'},
            {'name': 'valid_from', 'type': 'DATE'},
            {'name': 'valid_to', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'is_active', 'type': 'BOOLEAN'}
        ],
        time_partitioning={'type': 'DAY', 'field': 'valid_from'},
        gcp_conn_id='google_cloud_default',
    )

    update_cleaned_table_clustering = BigQueryUpdateTableOperator(
        task_id='update_cleaned_table_clustering',
        dataset_id='steam_data',
        table_id='cleaned_steam_games',
        project_id='stellar-river-464405-k3',
        fields=['clustering_fields'],
        table_resource={'clustering': {'fields': ['appid']}},
        gcp_conn_id='google_cloud_default',
    )

    extract_data_task = PythonOperator(
        task_id='extract_and_upload_to_gcs',
        python_callable=extract_and_upload,
        provide_context=True,
        op_kwargs={"is_initial_load": "{{ dag_run.conf.get('is_initial_load', False) }}"},
    )

    load_cleaned_task = GCSToBigQueryOperator(
        task_id='load_cleaned_to_bigquery',
        bucket='us-central1-composer-dev-33ed7046-bucket',
        source_objects=['processed/steam_games_cleaned_{{ ds }}*.json'],
        destination_project_dataset_table='stellar-river-464405-k3.steam_data.cleaned_steam_games_staging',
        source_format='NEWLINE_DELIMITED_JSON',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        ignore_unknown_values=True,
        autodetect=True,
        schema_update_options=['ALLOW_FIELD_ADDITION'],
        gcp_conn_id='google_cloud_default',
    )

    apply_scd_merge = BigQueryInsertJobOperator(
        task_id='apply_scd_merge',
        configuration={
            'query': {
                'query': '''
                    MERGE `stellar-river-464405-k3.steam_data.cleaned_steam_games` T
                    USING `stellar-river-464405-k3.steam_data.cleaned_steam_games_staging` S
                    ON T.appid = S.appid

                    WHEN MATCHED AND T.is_active = TRUE AND (
                        T.name != S.name OR
                        ARRAY_TO_STRING(T.genre, ',') != ARRAY_TO_STRING(S.genre, ',') OR
                        ARRAY_TO_STRING(T.tags, ',') != ARRAY_TO_STRING(S.tags, ',') OR
                        T.positive != S.positive OR
                        T.negative != S.negative OR
                        T.developer != S.developer OR
                        T.publisher != S.publisher OR
                        T.score_rank != S.score_rank OR
                        T.owners != S.owners OR
                        T.min_owners != S.min_owners OR
                        T.max_owners != S.max_owners OR
                        T.average_forever != S.average_forever OR
                        T.average_2weeks != S.average_2weeks OR
                        T.median_forever != S.median_forever OR
                        T.median_2weeks != S.median_2weeks OR
                        T.ccu != S.ccu OR
                        T.price != S.price OR
                        T.initialprice != S.initialprice OR
                        T.discount != S.discount OR
                        ARRAY_TO_STRING(T.languages, ',') != ARRAY_TO_STRING(S.languages, ',')
                    ) THEN
                        UPDATE SET T.valid_to = S.load_date, T.is_active = FALSE

                    WHEN NOT MATCHED THEN
                        INSERT (appid, name, genre, tags, positive, negative, developer, publisher,
                                score_rank, owners, min_owners, max_owners, average_forever, average_2weeks,
                                median_forever, median_2weeks, ccu, price, initialprice, discount, languages,
                                valid_from, valid_to, is_active)
                        VALUES (S.appid, S.name, S.genre, S.tags, S.positive, S.negative, S.developer,
                                S.publisher, S.score_rank, S.owners, S.min_owners, S.max_owners, S.average_forever,
                                S.average_2weeks, S.median_forever, S.median_2weeks, S.ccu, S.price,
                                S.initialprice, S.discount, S.languages, S.load_date, NULL, TRUE)
                ''',
                'useLegacySql': False,
            }
        },
        gcp_conn_id='google_cloud_default',
    )

    [create_staging_table, create_cleaned_table] >> update_cleaned_table_clustering >> extract_data_task >> load_cleaned_task >> apply_scd_merge