from datetime import datetime

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from includes.attributes import upload_attributes
from includes.broadcast_entries import upload_broadcast_entries
from includes.broadcasts_list import upload_broadcasts_list
from includes.segments_list import upload_segments_list
from includes.user_filters import upload_broadcast_recipients_and_audience_filter
from includes.users import upload_users


@task
def update_users():
    update_users = upload_users()


@task
def update_attributes():
    update_attributes = upload_attributes()


@task
def update_broadcast_list():
    update_bcast_list = upload_broadcasts_list()


@task
def update_broadcast_entries():
    update_bcast_entries = upload_broadcast_entries()


@task
def update_broadcast_recipients():
    update_bcast_recipients = upload_broadcast_recipients_and_audience_filter()


@task
def update_segments_table():
    update_segments = upload_segments_list()


@task_group
def pull_and_upload_data():
    [
        update_users(),
        update_attributes(),
        update_broadcast_list(),
        update_broadcast_entries(),
        update_segments_table(),
    ] >> update_broadcast_recipients()


@task.bash
def dbt_run_stg_cf_users_incremental():
    return "cd /opt/airflow/dbt/ && dbt run -s stg_cf_users_incremental --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_run_stg_cf_attributes_incremental():
    return "cd /opt/airflow/dbt/ && dbt run -s stg_cf_attributes_incremental --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_run_stg_cf_bcast_list_incremental():
    return "cd /opt/airflow/dbt/ && dbt run -s stg_cf_bcast_list_incremental --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_run_int_cf_attributes():
    return "cd /opt/airflow/dbt/ && dbt run -s int_cf_attributes --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_run_int_users_combined():
    return "cd /opt/airflow/dbt/ && dbt run -s int_cf_users_combined --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_run_users():
    return "cd /opt/airflow/dbt/ && dbt run -s users --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_run_stg_cf_segment_list_incremental():
    return "cd /opt/airflow/dbt/ && dbt run -s stg_cf_segment_list_incremental --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_run_stg_cf_user_filters_incremental():
    return "cd /opt/airflow/dbt/ && dbt run -s stg_cf_user_filters_incremental --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_run_int_cf_broadcast_audience():
    return "cd /opt/airflow/dbt/ && dbt run -s int_cf_broadcast_audience --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_run_int_cf_broadcast_list():
    return "cd /opt/airflow/dbt/ && dbt run -s int_cf_broadcast_list --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_run_broadcasts():
    return "cd /opt/airflow/dbt/ && dbt run -s broadcasts --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task_group
def dbt_jobs():
    (
        [dbt_run_stg_cf_users_incremental(), dbt_run_stg_cf_attributes_incremental()]
        >> dbt_run_int_cf_attributes()
        >> dbt_run_int_users_combined()
        >> dbt_run_users()
    )
    (
        [
            dbt_run_stg_cf_bcast_list_incremental(),
            dbt_run_stg_cf_segment_list_incremental(),
            dbt_run_stg_cf_user_filters_incremental(),
        ]
        >> dbt_run_int_cf_broadcast_audience()
        >> dbt_run_int_cf_broadcast_list()
        >> dbt_run_broadcasts()
    )


@task.bash
def dbt_test_stg_cf_users_incremental():
    return "cd /opt/airflow/dbt/ && dbt test --model stg_cf_users_incremental --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_test_stg_cf_attributes_incremental():
    return "cd /opt/airflow/dbt/ && dbt test --model stg_cf_attributes_incremental --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_test_stg_cf_bcast_list_incremental():
    return "cd /opt/airflow/dbt/ && dbt test --model stg_cf_bcast_list_incremental --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_test_stg_cf_user_filters_incremental():
    return "cd /opt/airflow/dbt/ && dbt test --model stg_cf_user_filters_incremental --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_test_stg_cf_segment_list_incremental():
    return "cd /opt/airflow/dbt/ && dbt test --model stg_cf_segment_list_incremental --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_test_int_cf_broadcast_audience():
    return "cd /opt/airflow/dbt/ && dbt test --model int_cf_broadcast_audience --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_test_int_cf_broadcast_list():
    return "cd /opt/airflow/dbt/ && dbt test --model int_cf_broadcast_list --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task.bash
def dbt_test_int_cf_users_combined():
    return "cd /opt/airflow/dbt/ && dbt test --model int_cf_users_combined --profiles-dir /opt/airflow/dbt/ --project-dir /opt/airflow/dbt/"


@task_group
def dbt_tests():
    [
        dbt_test_stg_cf_users_incremental(),
        dbt_test_stg_cf_attributes_incremental(),
        dbt_test_stg_cf_bcast_list_incremental(),
        dbt_test_stg_cf_user_filters_incremental(),
        dbt_test_stg_cf_segment_list_incremental(),
    ]
    [
        dbt_test_int_cf_broadcast_audience(),
        dbt_test_int_cf_broadcast_list(),
        dbt_test_int_cf_users_combined(),
    ]


@dag(schedule="30 7 * * *", start_date=datetime(2024, 6, 26), catchup=False)
def cf_data_pipeline():

    start_task = EmptyOperator(task_id="start")
    end_python_tasks = EmptyOperator(task_id="end_python_tasks")
    end_dbt_tasks = EmptyOperator(task_id="end_dbt_tasks")
    end_dbt_tests = EmptyOperator(task_id="end_dbt_tests")

    (
        start_task
        >> pull_and_upload_data()
        >> end_python_tasks
        >> dbt_jobs()
        >> end_dbt_tasks
        >> dbt_tests()
        >> end_dbt_tests
    )


cf_data_pipeline_dag = cf_data_pipeline()
