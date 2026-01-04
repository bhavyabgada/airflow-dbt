"""
================================================================================
DAG: uber_analytics_pipeline
Orchestration: Apache Airflow
================================================================================

PROBLEM SOLVED:
--------------------------------------------------------------------------------

#23 CIRCUIT BREAKER PATTERN
    Problem: Data quality issues can propagate through the pipeline:
             - Bad source data → Wrong staging
             - Wrong staging → Corrupted dimensions
             - Corrupted dimensions → Incorrect facts
             - Incorrect facts → Wrong business decisions!
             
             Without quality gates, bad data flows all the way to dashboards
             before anyone notices.
    
    Example Scenario:
    - Payment system had an outage, only 50% of payments loaded
    - Pipeline runs and calculates 50% match rate (should be 99%)
    - Finance dashboard shows revenue down 50%
    - CFO panics, stock price drops
    - Turns out data was just incomplete!
    
    Business Impact: Bad decisions based on incomplete data.
    
    Solution: CIRCUIT BREAKER
    - After critical steps, check data quality metrics
    - If metrics below threshold → STOP pipeline, alert team
    - Don't let bad data flow downstream
    - In Airflow: Use BranchPythonOperator to check quality
    
    Pipeline Flow:
    ```
    staging → quality_check_1 → integration → quality_check_2 → dimensions → facts → marts
                    ↓                              ↓
              STOP if bad                    STOP if bad
    ```

--------------------------------------------------------------------------------

DAG STRUCTURE:
    
    1. START: Validate connections, check source availability
    
    2. SEEDS: Load reference data (cities, currencies, rates)
       - Must succeed before any models
    
    3. STAGING: Create staging views
       - Parallel: all stg_* models together
       
    4. QUALITY GATE 1: Check staging completeness
       - Row counts meet minimum
       - Critical columns not all NULL
       
    5. SNAPSHOTS: Run SCD Type 2 snapshots
       - snap_driver, snap_rider
       
    6. INTEGRATION: Merge and cleanse data
       - int_trips_unified, int_payments_reconciled
       
    7. QUALITY GATE 2: Check reconciliation
       - Payment match rate >= 99%
       - Orphan rate <= 1%
       
    8. DIMENSIONS: Build dimension tables
       - Parallel: all dim_* models
       
    9. FACTS: Build fact tables
       - Parallel: all fct_* models
       
    10. MARTS: Build business aggregates
        - Parallel: all mart_* models
        
    11. TESTS: Run dbt tests
        - Fail pipeline if critical tests fail
        
    12. DOCS: Generate documentation
        - Refresh dbt docs site

--------------------------------------------------------------------------------

TASK DEPENDENCIES:
    
    start → seeds → staging → quality_gate_1
    quality_gate_1 (pass) → snapshots → integration → quality_gate_2
    quality_gate_1 (fail) → quality_failed → alert
    quality_gate_2 (pass) → dimensions → facts → marts → tests → docs
    quality_gate_2 (fail) → quality_failed → alert

================================================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# Configuration
DBT_PROJECT_DIR = '/Applications/MAMP/htdocs/DataEngineeringAcademy/DBT+Airflow/dbt/uber_analytics'
DBT_PROFILES_DIR = '/Applications/MAMP/htdocs/DataEngineeringAcademy/DBT+Airflow/dbt/uber_analytics/profiles'
VENV_ACTIVATE = 'source /Applications/MAMP/htdocs/DataEngineeringAcademy/DBT+Airflow/dbt/demo_dbt_env/bin/activate'

# Minimum match rate for quality gate (99%)
MIN_MATCH_RATE = 0.99

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

def check_quality_gate_1(**context):
    """
    CIRCUIT BREAKER: Check staging data quality.
    
    Validates:
    - Staging tables have minimum row counts
    - No critical columns are all NULL
    
    Returns task to execute next based on quality check result.
    """
    # In production, this would query the database
    # For demo, we assume quality is OK
    import random
    quality_ok = random.random() > 0.05  # 95% chance of passing
    
    if quality_ok:
        return 'snapshots.run_snapshots'
    else:
        return 'quality_failed'


def check_quality_gate_2(**context):
    """
    CIRCUIT BREAKER: Check reconciliation quality.
    
    Validates:
    - Payment match rate >= 99%
    - Orphan payment rate <= 1%
    
    This is the critical quality gate before building dimensions/facts.
    """
    # In production, query mart_reconciliation for actual metrics
    # Example:
    # SELECT avg(payment_match_rate) as match_rate
    # FROM mart_reconciliation
    # WHERE reconciliation_date = current_date - 1
    
    import random
    match_rate = random.uniform(0.97, 1.0)  # Simulate 97-100% match rate
    
    if match_rate >= MIN_MATCH_RATE:
        return 'dimensions.start_dimensions'
    else:
        return 'quality_failed'


with DAG(
    'uber_analytics_pipeline',
    default_args=default_args,
    description='Uber Analytics dbt Pipeline with Quality Gates',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['uber', 'dbt', 'analytics', 'production'],
    doc_md=__doc__,
) as dag:

    # =========================================================================
    # START
    # =========================================================================
    start = EmptyOperator(task_id='start')
    
    # =========================================================================
    # SEEDS: Load reference data
    # =========================================================================
    with TaskGroup(group_id='seeds') as seeds:
        run_seeds = BashOperator(
            task_id='run_seeds',
            bash_command=f'''
                {VENV_ACTIVATE} && \
                cd {DBT_PROJECT_DIR} && \
                dbt seed --profiles-dir {DBT_PROFILES_DIR}
            ''',
        )
    
    # =========================================================================
    # STAGING: Create staging views
    # =========================================================================
    with TaskGroup(group_id='staging') as staging:
        run_staging = BashOperator(
            task_id='run_staging',
            bash_command=f'''
                {VENV_ACTIVATE} && \
                cd {DBT_PROJECT_DIR} && \
                dbt run --select staging --profiles-dir {DBT_PROFILES_DIR}
            ''',
        )
    
    # =========================================================================
    # QUALITY GATE 1: Check staging completeness
    # =========================================================================
    quality_gate_1 = BranchPythonOperator(
        task_id='quality_gate_1',
        python_callable=check_quality_gate_1,
        doc_md="""
        **Circuit Breaker #1**
        
        Checks staging data quality before proceeding to snapshots/integration.
        Validates minimum row counts and column completeness.
        """
    )
    
    # =========================================================================
    # QUALITY FAILED: Stop pipeline
    # =========================================================================
    quality_failed = BashOperator(
        task_id='quality_failed',
        bash_command='echo "QUALITY CHECK FAILED! Pipeline stopped." && exit 1',
        trigger_rule='none_failed_min_one_success',
    )
    
    # =========================================================================
    # SNAPSHOTS: SCD Type 2
    # =========================================================================
    with TaskGroup(group_id='snapshots') as snapshots:
        run_snapshots = BashOperator(
            task_id='run_snapshots',
            bash_command=f'''
                {VENV_ACTIVATE} && \
                cd {DBT_PROJECT_DIR} && \
                dbt snapshot --profiles-dir {DBT_PROFILES_DIR}
            ''',
        )
    
    # =========================================================================
    # INTEGRATION: Merge and cleanse
    # =========================================================================
    with TaskGroup(group_id='integration') as integration:
        run_integration = BashOperator(
            task_id='run_integration',
            bash_command=f'''
                {VENV_ACTIVATE} && \
                cd {DBT_PROJECT_DIR} && \
                dbt run --select integration --profiles-dir {DBT_PROFILES_DIR}
            ''',
        )
    
    # =========================================================================
    # QUALITY GATE 2: Check reconciliation
    # =========================================================================
    quality_gate_2 = BranchPythonOperator(
        task_id='quality_gate_2',
        python_callable=check_quality_gate_2,
        doc_md="""
        **Circuit Breaker #2**
        
        Checks reconciliation quality (payment match rate >= 99%).
        This is the critical gate before building dimensions and facts.
        """
    )
    
    # =========================================================================
    # DIMENSIONS: Build dimension tables
    # =========================================================================
    with TaskGroup(group_id='dimensions') as dimensions:
        start_dimensions = EmptyOperator(task_id='start_dimensions')
        
        run_dimensions = BashOperator(
            task_id='run_dimensions',
            bash_command=f'''
                {VENV_ACTIVATE} && \
                cd {DBT_PROJECT_DIR} && \
                dbt run --select dimension --profiles-dir {DBT_PROFILES_DIR}
            ''',
        )
        
        start_dimensions >> run_dimensions
    
    # =========================================================================
    # FACTS: Build fact tables
    # =========================================================================
    with TaskGroup(group_id='facts') as facts:
        run_facts = BashOperator(
            task_id='run_facts',
            bash_command=f'''
                {VENV_ACTIVATE} && \
                cd {DBT_PROJECT_DIR} && \
                dbt run --select fact --profiles-dir {DBT_PROFILES_DIR}
            ''',
        )
    
    # =========================================================================
    # MARTS: Build business aggregates
    # =========================================================================
    with TaskGroup(group_id='marts') as marts:
        run_marts = BashOperator(
            task_id='run_marts',
            bash_command=f'''
                {VENV_ACTIVATE} && \
                cd {DBT_PROJECT_DIR} && \
                dbt run --select mart --profiles-dir {DBT_PROFILES_DIR}
            ''',
        )
    
    # =========================================================================
    # TESTS: Run dbt tests
    # =========================================================================
    with TaskGroup(group_id='tests') as tests:
        run_tests = BashOperator(
            task_id='run_tests',
            bash_command=f'''
                {VENV_ACTIVATE} && \
                cd {DBT_PROJECT_DIR} && \
                dbt test --profiles-dir {DBT_PROFILES_DIR}
            ''',
        )
    
    # =========================================================================
    # DOCUMENTATION: Generate dbt docs
    # =========================================================================
    with TaskGroup(group_id='documentation') as documentation:
        generate_docs = BashOperator(
            task_id='generate_docs',
            bash_command=f'''
                {VENV_ACTIVATE} && \
                cd {DBT_PROJECT_DIR} && \
                dbt docs generate --profiles-dir {DBT_PROFILES_DIR}
            ''',
        )
    
    # =========================================================================
    # END
    # =========================================================================
    end = EmptyOperator(task_id='end')
    
    # =========================================================================
    # TASK DEPENDENCIES
    # =========================================================================
    
    # Main flow
    start >> seeds >> staging >> quality_gate_1
    
    # Quality gate 1 branching
    quality_gate_1 >> snapshots >> integration >> quality_gate_2
    quality_gate_1 >> quality_failed
    
    # Quality gate 2 branching
    quality_gate_2 >> dimensions >> facts >> marts >> tests >> documentation >> end
    quality_gate_2 >> quality_failed
