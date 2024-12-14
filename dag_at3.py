import os
import logging
from datetime import datetime, timedelta
import pandas as pd
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


#########################################################
#
#   Load Environment Variables
#
#########################################################
#API_KEY= Variable.get("api_key") 


########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'PhuongThaoNguyen',
    'start_date': datetime.now(),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='BDE_AT3',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=5
)

#########################################################
#
#   Load Environment Variables
#
#########################################################
AIRFLOW_DATA = "/home/airflow/gcs/data"
DIMENSIONS_CENSUS_LGA = AIRFLOW_DATA+"/Census LGA/"
DIMENSIONS_NSW_LGA = AIRFLOW_DATA+"/NSW_LGA/"
FACTS = AIRFLOW_DATA+"/listings/"



#########################################################
#
#   Custom Logics for Operator
#
#########################################################


def import_load_dim_census_g01_func(**kwargs):
    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()


    #generate dataframe by combining files
    df = pd.read_csv(DIMENSIONS_CENSUS_LGA+'2016Census_G01_NSW_LGA.csv')

    if len(df) > 0:
        col_names = ','.join(list(df.columns))

        values = df.to_dict('split')
        values = values['data']

        insert_sql = """
                    INSERT INTO RAW.CENSUS_G01_NSW_LGA(%s)
                    VALUES %%s
                    """ % (col_names)

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None

def import_load_dim_census_g02_func(**kwargs):
    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()


    #generate dataframe by combining files
    df = pd.read_csv(DIMENSIONS_CENSUS_LGA+'2016Census_G02_NSW_LGA.csv')

    if len(df) > 0:
        col_names = ','.join(list(df.columns))

        values = df.to_dict('split')
        values = values['data']

        insert_sql = """
                    INSERT INTO RAW.CENSUS_G02_NSW_LGA(%s)
                    VALUES %%s
                    """ % (col_names)

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None

def import_load_dim_nsw_lga_code_func(**kwargs):
    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()


    #generate dataframe by combining files
    df = pd.read_csv(DIMENSIONS_NSW_LGA+'NSW_LGA_CODE.csv')

    if len(df) > 0:
        col_names = ','.join(list(df.columns))

        values = df.to_dict('split')
        values = values['data']

        insert_sql = """
                    INSERT INTO RAW.NSW_LGA_CODE(%s)
                    VALUES %%s
                    """ % (col_names)

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None

def import_load_dim_nsw_lga_suburb_func(**kwargs):
    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()


    #generate dataframe by combining files
    df = pd.read_csv(DIMENSIONS_NSW_LGA+'NSW_LGA_SUBURB.csv')

    if len(df) > 0:
        col_names = ['LGA_NAME', 'SUBURB_NAME']

        values = df[col_names].to_dict('split')
        values = values['data']

        insert_sql = """
                    INSERT INTO RAW.NSW_LGA_SUBURB(%s)
                    VALUES %%s
                    """ % (','.join(col_names))

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None

def import_load_listings_1_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    #get all files with filename including the string '.csv'
    filelist = [k for k in os.listdir(FACTS) if '.csv' in k][0:2]

    #generate dataframe by combining files
    df = pd.concat([pd.read_csv(os.path.join(FACTS, fname)) for fname in filelist], ignore_index=True)

    if len(df) > 0:
        col_names = ','.join(list(df.columns))

        values = df.to_dict('split')
        values = values['data']

        insert_sql = """
                    INSERT INTO RAW.LISTINGS(%s)
                    VALUES %%s
                    """ % (col_names)

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None

def import_load_listings_2_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    #get all files with filename including the string '.csv'
    filelist = [k for k in os.listdir(FACTS) if '.csv' in k][2:4]

    #generate dataframe by combining files
    df = pd.concat([pd.read_csv(os.path.join(FACTS, fname)) for fname in filelist], ignore_index=True)

    if len(df) > 0:
        col_names = ','.join(list(df.columns))

        values = df.to_dict('split')
        values = values['data']

        insert_sql = """
                    INSERT INTO RAW.LISTINGS(%s)
                    VALUES %%s
                    """ % (col_names)

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None

def import_load_listings_3_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    #get all files with filename including the string '.csv'
    filelist = [k for k in os.listdir(FACTS) if '.csv' in k][4:6]

    #generate dataframe by combining files
    df = pd.concat([pd.read_csv(os.path.join(FACTS, fname)) for fname in filelist], ignore_index=True)

    if len(df) > 0:
        col_names = ','.join(list(df.columns))

        values = df.to_dict('split')
        values = values['data']

        insert_sql = """
                    INSERT INTO RAW.LISTINGS(%s)
                    VALUES %%s
                    """ % (col_names)

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None

def import_load_listings_4_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    #get all files with filename including the string '.csv'
    filelist = [k for k in os.listdir(FACTS) if '.csv' in k][6:8]

    #generate dataframe by combining files
    df = pd.concat([pd.read_csv(os.path.join(FACTS, fname)) for fname in filelist], ignore_index=True)

    if len(df) > 0:
        col_names = ','.join(list(df.columns))

        values = df.to_dict('split')
        values = values['data']

        insert_sql = """
                    INSERT INTO RAW.LISTINGS(%s)
                    VALUES %%s
                    """ % (col_names)

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None

def import_load_listings_5_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    #get all files with filename including the string '.csv'
    filelist = [k for k in os.listdir(FACTS) if '.csv' in k][8:10]

    #generate dataframe by combining files
    df = pd.concat([pd.read_csv(os.path.join(FACTS, fname)) for fname in filelist], ignore_index=True)

    if len(df) > 0:
        col_names = ','.join(list(df.columns))

        values = df.to_dict('split')
        values = values['data']

        insert_sql = """
                    INSERT INTO RAW.LISTINGS(%s)
                    VALUES %%s
                    """ % (col_names)

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None

def import_load_listings_6_func(**kwargs):

    #set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    #get all files with filename including the string '.csv'
    filelist = [k for k in os.listdir(FACTS) if '.csv' in k][10:12]

    #generate dataframe by combining files
    df = pd.concat([pd.read_csv(os.path.join(FACTS, fname)) for fname in filelist], ignore_index=True)

    if len(df) > 0:
        col_names = ','.join(list(df.columns))

        values = df.to_dict('split')
        values = values['data']

        insert_sql = """
                    INSERT INTO RAW.LISTINGS(%s)
                    VALUES %%s
                    """ % (col_names)

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None

#########################################################
#
#   DAG Operator Setup
#
#########################################################


import_load_dim_census_g01_task = PythonOperator(
    task_id='import_load_dim_census_g01_id',
    python_callable=import_load_dim_census_g01_func,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    provide_context=True,
    dag=dag
)

import_load_dim_census_g02_task = PythonOperator(
    task_id='import_load_dim_census_g02_id',
    python_callable=import_load_dim_census_g02_func,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    provide_context=True,
    dag=dag
)

import_load_dim_nsw_lga_code_task = PythonOperator(
    task_id='import_load_dim_nsw_lga_code_id',
    python_callable=import_load_dim_nsw_lga_code_func,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    provide_context=True,
    dag=dag
)

import_load_dim_nsw_lga_suburb_task = PythonOperator(
    task_id='import_load_dim_nsw_lga_suburb_id',
    python_callable=import_load_dim_nsw_lga_suburb_func,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    provide_context=True,
    dag=dag
)

import_load_listings_1_task = PythonOperator(
    task_id='import_load_listings_1_id',
    python_callable=import_load_listings_1_func,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    provide_context=True,
    dag=dag
)

import_load_listings_2_task = PythonOperator(
    task_id='import_load_listings_2_id',
    python_callable=import_load_listings_2_func,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    provide_context=True,
    dag=dag
)

import_load_listings_3_task = PythonOperator(
    task_id='import_load_listings_3_id',
    python_callable=import_load_listings_3_func,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    provide_context=True,
    dag=dag
)

import_load_listings_4_task = PythonOperator(
    task_id='import_load_listings_4_id',
    python_callable=import_load_listings_4_func,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    provide_context=True,
    dag=dag
)

import_load_listings_5_task = PythonOperator(
    task_id='import_load_listings_5_id',
    python_callable=import_load_listings_5_func,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    provide_context=True,
    dag=dag
)

import_load_listings_6_task = PythonOperator(
    task_id='import_load_listings_6_id',
    python_callable=import_load_listings_6_func,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    provide_context=True,
    dag=dag
)
[import_load_dim_census_g01_task, import_load_dim_census_g02_task, import_load_dim_nsw_lga_code_task, import_load_dim_nsw_lga_suburb_task, import_load_listings_1_task, import_load_listings_2_task, import_load_listings_3_task, import_load_listings_4_task, import_load_listings_5_task, import_load_listings_6_task]



