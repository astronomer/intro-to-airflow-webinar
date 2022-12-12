from airflow import DAG 
from datetime import date, datetime

from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator, SQLTableCheckOperator
)
from airflow.operators.sql import SQLCheckOperator

with DAG(
    dag_id="data_quality_example_dag",
    start_date=datetime(2022,7,1),
    schedule_interval=None,
    catchup=False
) as dag:

    column_checks = SQLColumnCheckOperator(
        task_id="column_checks",
        conn_id="snowflake_conn",
        table="ORDERS",
        column_mapping={
            "O_ORDERDATE": {
                "min": {"greater_than": date(1980, 1, 1)},
                "max": {"less_than": date(2022, 1, 1)}
            },
            "O_COMMENT": {
                "distinct_check": {"geq_to": 10},
                "null_check": {"equal_to": 0}
            },
            "O_TOTALPRICE": {
                "min": {"greater_than": 0, "tolerance": 0.1}
            },
        }
    )

    # SQLTableCheckOperator example: This Operator performs two checks:
    #   - a row count check, making sure the table has >= 1000 rows
    #   - a columns sum comparison check to the sum of MY_COL_1 is below the
    #     sum of MY_COL_2
    table_checks = SQLTableCheckOperator(
        task_id="table_checks",
        conn_id="snowflake_conn",
        table="ORDERS",
        checks={
            "my_row_count_check": {
                "check_statement": "COUNT(*) >= 1000"
                },
            "my_column_sum_comparison_check": {
                "check_statement": "SUM(O_CUSTKEY) < SUM(O_TOTALPRICE)"
                }
        }
    )

    # SQLCheckOperator example: ensure categorical values in MY_COL_3
    # are one of a list of 4 options
    check_val_in_bounds = SQLCheckOperator(
        task_id="check_today_val_in_bounds",
        conn_id="snowflake_conn",
        sql="""
                WITH

                not_in_list AS (

                SELECT COUNT(*) as count_not_in_list
                FROM {{ params.db_to_query }}.{{ params.schema }}.\
                     {{ params.table }}
                WHERE {{ params.col }} NOT IN {{ params.options_tuple }}
                )

                SELECT
                    CASE WHEN count_not_in_list = 0 THEN 1
                    ELSE 0
                    END AS testresult
                FROM not_in_list
            """,
        params={"db_to_query": "SNOWFLAKE_SAMPLE_DATA",
                "schema": "TPCH_SF1",
                "table": "ORDERS",
                "col": "O_ORDERSTATUS",
                "options_tuple": "('F', 'O', 'P')"
                }
    )
    
    column_checks >> table_checks >> check_val_in_bounds