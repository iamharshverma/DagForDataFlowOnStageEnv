
# Dag to Run Data on Stage and Evaluate the final results of scores from pyspark parallel computation
# @author Harsh Verma

from airflow.models import Variable
from string import Template
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator,DataProcPySparkOperator,DataprocClusterCreateOperator,DataprocClusterDeleteOperator
import logging
from airflow.operators.python_operator import PythonOperator
import time


config = Variable.get("univ_search_staging_config", deserialize_json=True)
default_args = {
    'owner': 'Data Science',
    'start_date': datetime(2018, 2, 5, hour=7, minute=30),
    'email': [‘harshverma14oct@gmail.com'],
    'nagios_alert_on_failure': False,
    'nagios_alert_on_retry': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1
}
dag = DAG(dag_id='MABDiscoverPreAgg-staging', schedule_interval='0 7,11,15,19 * * *', default_args=default_args,
          max_active_runs=1)


# endpointDay = "{{(execution_date + macros.timedelta(1)).strftime('%d')}}"
# endpointHour = "{{(execution_date + macros.timedelta(1)).strftime('%H')}}"
# endpointMinute = "{{(execution_date + macros.timedelta(1)).strftime('%M')}}"


# Define parameters for remaining DAG nodes
def define_task_params(**kwargs):
    date_time_obj = kwargs["execution_date"]
    logging.info(date_time_obj)
    start_date = date_time_obj.strftime('%Y_%-m_%-d')
    start_hour = date_time_obj.strftime('%-H')
    gslocation_user_affinity_data = "gs://stage/v1/simulation/activity_data/dt=" + start_date + "/hour=" + start_hour
    endpoint_date = date_time_obj.strftime('%Y-%m-%d')
    from_date = int(time.mktime((date_time_obj - timedelta(days=15)).timetuple()))
    to_date = int(time.mktime(date_time_obj.timetuple()))
    params = {'endpoint_date':endpoint_date, 'gslocation_user_affinity_data':gslocation_user_affinity_data, 'from_date':from_date, 'to_date':to_date}
    logging.info(params)
    return params


define_task_params_task = PythonOperator(
    task_id='define-task-params',
    provide_context=True,
    python_callable=define_task_params,
    dag=dag
)

#query 1

hive_query_staging_feedback_generator_tmp = Template('''
create database if not exists mab;
Drop table if exists mab.staging_content_ranking_mab_feedback_tmp;
Create EXTERNAL table if not exists mab.staging_content_ranking_mab_feedback_tmp
( 
item STRING, 
viewer STRING, 
isliked int,
creator STRING, 
ts bigint 
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
location '${gslocation_user_affinity_data}'; 
''').substitute({"gslocation_user_affinity_data": "{{ ti.xcom_pull(task_ids='define-task-params')['gslocation_user_affinity_data'] }}"})

hive_job_staging_feedback_generator_tmp = DataProcHiveOperator(
    task_id='staging_content_ranking_mab_feedback_tmp',
    dataproc_cluster=config['univ_search_staging_cluster'],
    gcp_conn_id=config["gcp_conn_id"],
    query=hive_query_staging_feedback_generator_tmp,
    dag=dag
)

#query 2

gslocation_feedback_table = 'gs://hike_staging/v1/simulation/feedback_table'
hive_query_staging_feedback_generator = Template('''
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
CREATE EXTERNAL TABLE IF NOT EXISTS mab.staging_content_ranking_mab_feedback
(ts timestamp,
context string,
item string,
rewards double,
trials double)
partitioned by (app_name string,dt bigint)
stored as parquet
LOCATION '${gslocation_feedback_table}'; 

drop table if exists mab.staging_content_ranking_mab_feedback_tmp_today;

create table mab.staging_content_ranking_mab_feedback_tmp_today AS
Select CAST(CAST('${endpoint_date}' as date) as timestamp) as ts,
"global" as context, item, sum(IF(isliked = 1, 1, 0)) as rewards ,
COUNT(*) as trials,"discover_post" as app_name, cast('${to_date}' as BIGINT) as dt 
FROM mab.staging_content_ranking_mab_feedback_tmp GROUP BY item;

INSERT OVERWRITE TABLE mab.staging_content_ranking_mab_feedback partition (app_name, dt)
select * from mab.staging_content_ranking_mab_feedback_tmp_today;
''').substitute({"gslocation_feedback_table": gslocation_feedback_table, 'endpoint_date': "{{ ti.xcom_pull(task_ids='define-task-params')['endpoint_date'] }}",
                 'to_date': "{{ ti.xcom_pull(task_ids='define-task-params')['to_date'] }}" })

hive_job_staging_feedback_generator = DataProcHiveOperator(
    task_id='staging_content_ranking_mab_feedback',
    dataproc_cluster=config['univ_search_staging_cluster'],
    gcp_conn_id=config["gcp_conn_id"],
    query=hive_query_staging_feedback_generator,
    dag=dag
)

#query 3

gslocation_prior_table = 'gs://hike_staging/v1/simulation/prior_table'
hive_query_staging_prior_generator = Template('''
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
CREATE EXTERNAL TABLE IF NOT EXISTS mab.staging_content_ranking_mab_prior
(context string,
item String,
alpha double,
beta double,
enabled boolean)
partitioned by (app_name string, dt bigint)
stored as parquet
LOCATION '${gslocation_prior_table}';

INSERT OVERWRITE TABLE mab.staging_content_ranking_mab_prior partition (app_name, dt)
Select "global" as context, t2.item,cast(1 as double) as alpha,
cast(100 as double) as beta,true as enabled, "discover_post" as app_name, cast('${to_date}' as BIGINT) as dt  from
mab.staging_content_ranking_mab_prior as t1 right outer join mab.staging_content_ranking_mab_feedback_tmp_today as t2 on (t1.item = t2.item)
where t1.item is null;
''').substitute({"gslocation_prior_table": gslocation_prior_table, 'endpoint_date': "{{ ti.xcom_pull(task_ids='define-task-params')['endpoint_date'] }}",
                 'to_date': "{{ ti.xcom_pull(task_ids='define-task-params')['to_date'] }}"})

hive_job_staging_prior_generator = DataProcHiveOperator(
    task_id='staging_content_ranking_mab_prior',
    dataproc_cluster=config['univ_search_staging_cluster'],
    gcp_conn_id=config["gcp_conn_id"],
    query=hive_query_staging_prior_generator,
    dag=dag
)


#Pyspark socring job

argument1 = Template('''-s${fromDate}''').substitute({'fromDate':"{{ ti.xcom_pull(task_ids='define-task-params')['from_date'] }}"})
argument2 = Template('''-e${toDate}''').substitute({'toDate':"{{ ti.xcom_pull(task_ids='define-task-params')['to_date'] }}"})
argument3 = '''-rmab.staging_content_ranking_mab_prior'''
argument4 = '''-fmab.staging_content_ranking_mab_feedback'''
argument5 = '''-k20'''
argument6 = '''-adiscover_post'''
argument7 = '''-b900000'''
argument8 = '''-p'''
argument9 = '''-t'''


mab_post_discover = DataProcPySparkOperator(
    main="gs://stage/mab/scorerJob/mabScorerJob.py",
    job_name='mab_post_discover',
    arguments=[argument1, argument2, argument3, argument4, argument5, argument6, argument7, argument8, argument9],
    dataproc_cluster=config['univ_search_staging_cluster'],
    gcp_conn_id=config["gcp_conn_id"],
    task_id="mab_post_discover",
    dag=dag)


define_task_params_task >> hive_job_staging_feedback_generator_tmp >> \
hive_job_staging_feedback_generator >> hive_job_staging_prior_generator >> mab_post_discover