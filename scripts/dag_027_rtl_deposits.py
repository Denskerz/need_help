import gin
from bicc_ml_pipeline.airflow.dag_utils import init_dag_gin_ext, make_ml_pipeline_dag, update_dag_kvargs


@gin.configurable
def make_dag(dag_kvargs,
             start_date,
             emails_on_success,
            ):
    dag_id = dag_kvargs['dag_id']
    dag_kvargs = update_dag_kvargs(dag_kvargs, start_date)

    root_path = f'/ml_data/{dag_id}'
    config_path = f'{root_path}/configs'
    inference_cmd = f'python3 {root_path}/scripts/main.py'

    dag = make_ml_pipeline_dag(
        CONFIG_PATH, dag_kvargs, emails_on_success,
        clean_folders_cmd=f"sh {root_path}/scripts/clean.sh ",
        download_data_cmd=f"python {root_path}/scripts/read_teradata.py -p {config_path}",
        run_model_cmd=f"sudo sh {root_path}/docker/run.sh '{inference_cmd}'",
        inference_cmd=inference_cmd,
        upload_predictions_cmd=f"python {root_path}/scripts/upload_predictions.py -p {config_path}",
        monitor_metrics_cmd=None,
        upload_monitor_metrics_cmd=None,
        check_teradata_cmd=None,
    )
    return dag, dag_id


CONFIG_PATH = init_dag_gin_ext(__name__, 'configs', 'config_dag.gin')
dag, dag_id = make_dag()

globals()[dag_id] = dag
