from airflow import DAG
from google.cloud.dataform_v1beta1.types import WorkflowInvocation
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformGetCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor
from datetime import timedelta
from core.zillow.configurations import settings


# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create the DAG
with DAG(
    'zillow_dataform',
    default_args=default_args,
    description='DAG to fetch and process Zillow data with dynamic region support',
    schedule_interval=None,
    start_date=None,
    catchup=False,

) as dag:
    
    # Task 1: Create a Dataform compilation result
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id='create_compilation_result',
        project_id=settings.PROJECT_ID,
        region="us-east1",
        repository_id=settings.REPOSITORY_ID,
        compilation_result={
            "git_commitish": "main",
            "workspace": (
                f"projects/{settings.PROJECT_ID}/locations/us-east1/repositories/{settings.REPOSITORY_ID}/"
                f"workspaces/{settings.WORKSPACE_ID}"
            ),
        }
    )

    # Task 2: Get the Dataform compilation result
    get_compilation_result = DataformGetCompilationResultOperator(
        task_id='get_compilation_result',
        project_id=settings.PROJECT_ID,
        region="us-east1",
        repository_id=settings.REPOSITORY_ID,
        compilation_result_id="{{ task_instance.xcom_pull('create_compilation_result')['name'].split('/')[-1] }}",
    )

    # Task 3: Create a Dataform workflow invocation
    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id=settings.PROJECT_ID,
        region="us-east1",
        repository_id=settings.REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
        },
    )

    # Task 4: Monitor the Dataform workflow invocation until completion
    monitor_workflow_invocation = DataformWorkflowInvocationStateSensor(
        task_id='monitor_workflow_invocation',
        project_id=settings.PROJECT_ID,
        region="us-east1",
        repository_id=settings.REPOSITORY_ID,
        workflow_invocation_id="{{ task_instance.xcom_pull('create_workflow_invocation')['name'].split('/')[-1] }}",
        timeout=3600,  # Timeout in seconds
        poke_interval=5,  # Interval between status checks
        expected_statuses={WorkflowInvocation.State.SUCCEEDED}
    )

    create_compilation_result >> get_compilation_result >> create_workflow_invocation >> monitor_workflow_invocation