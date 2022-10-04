from __future__ import annotations


import json
from textwrap import dedent
import pendulum
import pandas as pd
import featuretools as ft
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

# [END import_module]

# [START instantiate_dag]
with DAG(
    'Feature_Store',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={'retries': 2},
    # [END default_args]
    description='Feature Store',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['fraud_detection'],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [START extract_function]
    def extract(**kwargs):
        ti = kwargs['ti']
        data = "/Users/pushpanath/venv/lib/python3.10/site-packages/airflow/example_dags/input.csv"
        ti.xcom_push('input_data', data)

    # [END extract_function]

    # [START transform_function]
    def transform(**kwargs):
        ti = kwargs['ti']
        extract_data = ti.xcom_pull(task_ids='extract', key='input_data')
        print(extract_data)
        data = pd.read_csv(extract_data)
        print('data size', data.shape)
        data_Item_Identifier = data['cc_num']
        data_Outlet_Identifier = data['zip']
        amount_spent = data['amt']
        gender_content_dict = {'F': 0, 'M': 1}
        data['gender'] = data['gender'].replace(gender_content_dict, regex=True)
        data['id'] = data['trans_num']
        es = ft.EntitySet(id='fraud_feature')
        es.add_dataframe(dataframe_name="data", dataframe=data, index="id", time_index="trans_date_trans_time",
                     logical_types="")
        es.normalize_dataframe("data", "feature_dataframe", "trans_num", additional_columns=None, copy_columns=None,
                           make_time_index=None, make_secondary_time_index=None, new_dataframe_time_index=None,
                           new_dataframe_secondary_time_index=None)
        feature_matrix, feature_names = ft.dfs(dataframes=data, relationships=None, entityset=es,
                                           target_dataframe_name="feature_dataframe", cutoff_time=None,
                                           instance_ids=None, agg_primitives=None, trans_primitives=None,
                                           groupby_trans_primitives=None, allowed_paths=None, max_depth=2,
                                           ignore_dataframes=None, ignore_columns=None, primitive_options=None,
                                           seed_features=None, drop_contains=None, drop_exact=None,
                                           where_primitives=None, max_features=-1, cutoff_time_in_index=False,
                                           save_progress=None, features_only=False, training_window=None,
                                           approximate=None, chunk_size=None, n_jobs=1, dask_kwargs=None,
                                           verbose=False, return_types=None, progress_callback=None,
                                           include_cutoff_time=True)
        feature_matrix.to_csv("/Users/pushpanath/venv/lib/python3.10/site-packages/airflow/example_dags/feature_store.csv", encoding='utf-8')
        output_data = "/Users/pushpanath/venv/lib/python3.10/site-packages/airflow/example_dags/feature_store.csv"
        print(output_data)
        ti.xcom_push('transform_data', output_data)
        
    
    # [END transform_function]



    # [START load_function]
    def load(**kwargs):
        ti = kwargs['ti']
        output_data = ti.xcom_pull(task_ids='transform', key='transform_data')
        feature_store = pd.read_csv(output_data)
        print(feature_store)

    # [END load_function]

    # [START main_flow]
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    extract_task.doc_md = dedent(
        """\
    #### Extract task
    A simple Extract task to get data ready for the rest of the data pipeline.
    In this case, getting data is simulated by reading from a hardcoded JSON string.
    This data is then put into xcom, so that it can be processed by the next task.
    """
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    transform_task.doc_md = dedent(
        """\
    #### Transform task
    A simple Transform task which takes in the collection of order data from xcom
    and computes the total order value.
    This computed value is then put into xcom, so that it can be processed by the next task.
    """
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    load_task.doc_md = dedent(
        """\
    #### Load task
    A simple Load task which takes in the result of the Transform task, by reading it
    from xcom and instead of saving it to end user review, just prints it out.
    """
    )

    extract_task >> transform_task >> load_task

# [END main_flow]

# [END tutorial]
