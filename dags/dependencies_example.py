from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import cross_downstream

with DAG(
    dag_id="dependencies_example",
    start_date=datetime(2022,7,1),
    schedule_interval=None,
    catchup=False
) as dag:

    tt1 = EmptyOperator(task_id="extract")
    tt2 = EmptyOperator(task_id="transform_parallel_step_1")
    tt3 = EmptyOperator(task_id="transform_parallel_step_2")
    tt4 = EmptyOperator(task_id="transform_parallel_step_3")
    tt5 = EmptyOperator(task_id="transform_parallel_step_4")

    with TaskGroup(group_id='model_group_id') as model_group:
        t1 = EmptyOperator(task_id="transform_sequential_step_1")
        t2 = EmptyOperator(task_id="transform_sequential_step_2")
        t3 = EmptyOperator(task_id="transform_sequential_step_3")
        t4 = EmptyOperator(task_id="parallel_step_1")
        t5 = EmptyOperator(task_id="parallel_step_2")

        t1 >> t2 >> t3 >> [t4, t5]

    tt6 = EmptyOperator(task_id="branch_1")
    tt7 = EmptyOperator(task_id="branch_2")

    with TaskGroup(group_id='group_branch_1_id') as group_branch_1:
        t1 = EmptyOperator(task_id="load_1")
        t2 = EmptyOperator(task_id="load_2")

        t1 >> t2

    with TaskGroup(group_id='group_branch_2_id') as group_branch_2:
        t1 = EmptyOperator(task_id="load_1")
        t2 = EmptyOperator(task_id="load_2")
        t3 = EmptyOperator(task_id="load_3")
        t4 = EmptyOperator(task_id="load_4")
        t5 = EmptyOperator(task_id="load_5")
        t6 = EmptyOperator(task_id="load_6")

        cross_downstream([t1, t2, t3], [t4, t5, t6])

    tt8 = EmptyOperator(task_id="end")
    tt9 = EmptyOperator(task_id="leaf_task")

    tt1 >> [tt2, tt3, tt4, tt5] >> model_group >> [tt6, tt7]

    tt3 >> tt9

    tt6 >> group_branch_1
    tt7 >> group_branch_2

    group_branch_1 >> tt8
    group_branch_2 >> tt8


