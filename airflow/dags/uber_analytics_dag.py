"""
Uber Analytics Pipeline - Production Ready DAG
===============================================

Orchestrates the Uber Analytics data warehouse pipeline demonstrating
34 real-world data engineering challenges.

Architecture:
    Seeds → Snapshots → Staging → Integration → Quality Gate →
    Dimensions → Facts → Quality Gate → Marts → Tests → Docs

Author: Data Engineering Team
Version: 1.0.0
"""

import os
import json
from datetime import datetime, timedelta
from typing import Dict, List

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# =============================================================================
# Configuration
# =============================================================================

PROJECT_ROOT = "/Applications/MAMP/htdocs/DataEngineeringAcademy/DBT+Airflow"
DBT_PROJECT_PATH = os.path.join(PROJECT_ROOT, "dbt/uber_analytics")
DBT_VENV_PATH = os.path.join(PROJECT_ROOT, "dbt/demo_dbt_env")

DBT_CMD_PREFIX = f"source {DBT_VENV_PATH}/bin/activate && cd {DBT_PROJECT_PATH}"

DEFAULT_ARGS = {
    'owner': 'uber-data-engineering',
    'depends_on_past': False,
    'email': ['data-alerts@uber.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'execution_timeout': timedelta(hours=2),
}

# =============================================================================
# Helper Functions
# =============================================================================

def get_dbt_cmd(command: str, select: str = None, vars: Dict = None) -> str:
    """Generate dbt command string."""
    cmd = f"{DBT_CMD_PREFIX} && dbt {command}"
    if select:
        cmd += f" --select {select}"
    if vars:
        vars_str = json.dumps(vars).replace('"', '\\"')
        cmd += f' --vars "{vars_str}"'
    return cmd


def check_quality_gate(**context) -> str:
    """
    Check data quality and decide whether to proceed.
    Returns next task group based on quality checks.
    """
    # In production, query mart_reconciliation for circuit_breaker_triggered
    # For now, always proceed
    return 'dimensions.start_dimensions'


def notify_failure(context):
    """Send failure notification."""
    task = context['task_instance']
    message = f"""
    ⚠️ Uber Analytics Pipeline Failed
    Task: {task.task_id}
    DAG: {task.dag_id}
    Execution: {context['execution_date']}
    """
    print(message)
    # In production: send to Slack/PagerDuty


def notify_success(context):
    """Send success notification."""
    message = f"""
    ✅ Uber Analytics Pipeline Completed
    DAG: {context['dag_id']}
    Execution: {context['execution_date']}
    """
    print(message)


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id='uber_analytics_pipeline',
    description='Production Uber Analytics pipeline demonstrating 34 DE challenges',
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2024, 1, 1, tz='UTC'),
    schedule_interval='0 */4 * * *',  # Every 4 hours
    catchup=False,
    max_active_runs=1,
    tags=['uber', 'analytics', 'dbt', 'production'],
    doc_md=__doc__,
    on_failure_callback=notify_failure,
    on_success_callback=notify_success,
) as dag:

    # =========================================================================
    # Pipeline Start/End
    # =========================================================================
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    # =========================================================================
    # Seeds - Reference Data
    # =========================================================================
    
    with TaskGroup(group_id='seeds') as seed_group:
        seed_start = EmptyOperator(task_id='start_seeds')
        
        load_seeds = BashOperator(
            task_id='load_reference_data',
            bash_command=get_dbt_cmd('seed'),
            doc_md="Load reference data: cities, currencies, service types"
        )
        
        seed_end = EmptyOperator(task_id='end_seeds')
        seed_start >> load_seeds >> seed_end

    # =========================================================================
    # Snapshots - SCD Type 2
    # =========================================================================
    
    with TaskGroup(group_id='snapshots') as snapshot_group:
        snap_start = EmptyOperator(task_id='start_snapshots')
        
        snap_driver = BashOperator(
            task_id='snap_driver',
            bash_command=get_dbt_cmd('snapshot', 'snap_driver'),
            doc_md="SCD Type 2 snapshot for driver dimension"
        )
        
        snap_rider = BashOperator(
            task_id='snap_rider',
            bash_command=get_dbt_cmd('snapshot', 'snap_rider'),
            doc_md="SCD Type 2 snapshot for rider dimension"
        )
        
        snap_end = EmptyOperator(task_id='end_snapshots')
        snap_start >> [snap_driver, snap_rider] >> snap_end

    # =========================================================================
    # Staging - Source Ingestion
    # =========================================================================
    
    with TaskGroup(group_id='staging') as staging_group:
        stg_start = EmptyOperator(task_id='start_staging')
        
        stg_trips = BashOperator(
            task_id='stg_trips',
            bash_command=get_dbt_cmd('run', 'stg_trips_driver_app stg_trips_rider_app'),
            doc_md="Stage trip data from driver and rider apps"
        )
        
        stg_entities = BashOperator(
            task_id='stg_entities',
            bash_command=get_dbt_cmd('run', 'stg_drivers stg_riders stg_vehicles'),
            doc_md="Stage driver, rider, and vehicle data"
        )
        
        stg_other = BashOperator(
            task_id='stg_other',
            bash_command=get_dbt_cmd('run', 'stg_payments stg_surge_snapshots'),
            doc_md="Stage payments and surge data"
        )
        
        stg_end = EmptyOperator(task_id='end_staging')
        stg_start >> [stg_trips, stg_entities, stg_other] >> stg_end

    # =========================================================================
    # Integration - Data Quality & Merging
    # =========================================================================
    
    with TaskGroup(group_id='integration') as integration_group:
        int_start = EmptyOperator(task_id='start_integration')
        
        int_trips = BashOperator(
            task_id='int_trips_unified',
            bash_command=get_dbt_cmd(
                'run', 
                'int_trips_unified',
                {'execution_date': '{{ ds }}'}
            ),
            doc_md="Unify trips: dedup, late arrivals, currency conversion"
        )
        
        int_payments = BashOperator(
            task_id='int_payments_reconciled',
            bash_command=get_dbt_cmd('run', 'int_payments_reconciled'),
            doc_md="Reconcile payments with trips"
        )
        
        int_end = EmptyOperator(task_id='end_integration')
        int_start >> int_trips >> int_payments >> int_end

    # =========================================================================
    # Quality Gate 1 - Pre-Dimension
    # =========================================================================
    
    quality_gate_1 = BranchPythonOperator(
        task_id='quality_gate_1',
        python_callable=check_quality_gate,
        doc_md="Check data quality before building dimensions"
    )
    
    quality_failed = EmptyOperator(
        task_id='quality_failed',
        doc_md="Quality gate failed - investigate data issues"
    )

    # =========================================================================
    # Dimensions
    # =========================================================================
    
    with TaskGroup(group_id='dimensions') as dimension_group:
        dim_start = EmptyOperator(task_id='start_dimensions')
        
        dim_date = BashOperator(
            task_id='dim_date',
            bash_command=get_dbt_cmd('run', 'dim_date'),
            doc_md="Build date dimension with fiscal calendars"
        )
        
        dim_geo = BashOperator(
            task_id='dim_geography',
            bash_command=get_dbt_cmd('run', 'dim_geography'),
            doc_md="Build geography hierarchy dimension"
        )
        
        dim_driver = BashOperator(
            task_id='dim_driver',
            bash_command=get_dbt_cmd('run', 'dim_driver'),
            doc_md="Build driver SCD Type 2 dimension"
        )
        
        dim_rider = BashOperator(
            task_id='dim_rider',
            bash_command=get_dbt_cmd('run', 'dim_rider'),
            doc_md="Build rider SCD Type 2 dimension"
        )
        
        dim_other = BashOperator(
            task_id='dim_other',
            bash_command=get_dbt_cmd('run', 'bridge_driver_vehicle dim_trip_flags'),
            doc_md="Build bridge and junk dimensions"
        )
        
        dim_end = EmptyOperator(task_id='end_dimensions')
        
        dim_start >> dim_date >> dim_geo
        dim_start >> [dim_driver, dim_rider] >> dim_other >> dim_end
        dim_geo >> dim_end

    # =========================================================================
    # Facts
    # =========================================================================
    
    with TaskGroup(group_id='facts') as fact_group:
        fct_start = EmptyOperator(task_id='start_facts')
        
        fct_trips = BashOperator(
            task_id='fct_trips',
            bash_command=get_dbt_cmd(
                'run', 
                'fct_trips',
                {'execution_date': '{{ ds }}'}
            ),
            doc_md="Build transaction grain trip fact"
        )
        
        fct_accumulating = BashOperator(
            task_id='fct_trip_accumulating',
            bash_command=get_dbt_cmd('run', 'fct_trip_accumulating'),
            doc_md="Build accumulating snapshot fact"
        )
        
        fct_earnings = BashOperator(
            task_id='fct_driver_earnings',
            bash_command=get_dbt_cmd('run', 'fct_driver_earnings'),
            doc_md="Build driver earnings periodic snapshot"
        )
        
        fct_surge = BashOperator(
            task_id='fct_surge_snapshot',
            bash_command=get_dbt_cmd('run', 'fct_surge_snapshot'),
            doc_md="Build surge pricing snapshots"
        )
        
        fct_end = EmptyOperator(task_id='end_facts')
        
        fct_start >> fct_trips >> [fct_accumulating, fct_earnings] >> fct_end
        fct_start >> fct_surge >> fct_end

    # =========================================================================
    # Marts
    # =========================================================================
    
    with TaskGroup(group_id='marts') as mart_group:
        mart_start = EmptyOperator(task_id='start_marts')
        
        mart_revenue = BashOperator(
            task_id='mart_revenue_daily',
            bash_command=get_dbt_cmd('run', 'mart_revenue_daily'),
            doc_md="Build daily revenue mart with fiscal calendar"
        )
        
        mart_driver = BashOperator(
            task_id='mart_driver_performance',
            bash_command=get_dbt_cmd('run', 'mart_driver_performance'),
            doc_md="Build driver performance mart"
        )
        
        mart_recon = BashOperator(
            task_id='mart_reconciliation',
            bash_command=get_dbt_cmd('run', 'mart_reconciliation'),
            doc_md="Build reconciliation report"
        )
        
        mart_end = EmptyOperator(task_id='end_marts')
        mart_start >> [mart_revenue, mart_driver, mart_recon] >> mart_end

    # =========================================================================
    # Tests & Documentation
    # =========================================================================
    
    with TaskGroup(group_id='quality_tests') as test_group:
        test_start = EmptyOperator(task_id='start_tests')
        
        run_tests = BashOperator(
            task_id='run_all_tests',
            bash_command=get_dbt_cmd('test'),
            doc_md="Run all dbt data quality tests"
        )
        
        generate_docs = BashOperator(
            task_id='generate_docs',
            bash_command=get_dbt_cmd('docs generate'),
            doc_md="Generate dbt documentation"
        )
        
        test_end = EmptyOperator(task_id='end_tests')
        test_start >> run_tests >> generate_docs >> test_end

    # =========================================================================
    # Task Dependencies
    # =========================================================================
    
    # Main flow
    start >> seed_group >> snapshot_group >> staging_group >> integration_group
    integration_group >> quality_gate_1
    
    # Quality gate branching
    quality_gate_1 >> [dimension_group, quality_failed]
    quality_failed >> end
    
    # Continue main flow
    dimension_group >> fact_group >> mart_group >> test_group >> end

