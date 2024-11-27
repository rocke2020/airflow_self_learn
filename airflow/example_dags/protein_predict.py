""" Author: Chenhao """
import json
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag
import mixbio.common.protein_external_api as protein_external_api

# data = requests.get("http://crispr-mining-server.beta.aigene.org.cn/internal/api/v1/airflow/dag/protein_predict",
#                     timeout=5)
# if data.status_code != 200:
#     max_runs = 10
# else:
#     result = json.loads(data.content.decode())
#     max_runs = result["maxActiveRuns"]


@dag(schedule_interval=None, start_date=datetime(2022, 1, 7), catchup=False, tags=['protein'],
     max_active_runs=10, default_args={'retries': 3, 'retry_delay': timedelta(seconds=30)})
def protein_predict():
    import logging
    import os
    from airflow.decorators import task
    from airflow.exceptions import AirflowFailException
    from airflow.models import Variable
    from airflow.models.baseoperator import chain
    from airflow.operators.python import BranchPythonOperator, get_current_context
    """
    protein structure predict
    """

    @task()
    def generate_fasta(**kwargs):
        from mixbio.common.fasta import Fasta
        fasta_head = kwargs['dag_run'].conf.get("fasta_head")
        fasta_sequence = kwargs['dag_run'].conf["fasta_sequence"]
        protein_structure_id = kwargs['dag_run'].conf["protein_structure_id"]
        task_type = kwargs['dag_run'].conf["task_type"]

        fasta = Fasta(fasta_head, fasta_sequence.upper())
        fasta_path = Variable.get("alphafold_input_path", default_var="output")
        fasta_file = "%s.fasta" % str(protein_structure_id)
        fasta.generate_file(fasta_path, fasta_file, task_type)
        return os.path.join(fasta_path, fasta_file)

    @task
    def trigger_protein_predict_job(fasta_file: str, **kwargs):
        from mixbio.common.oss_client import OssClient

        """
        upload oss fasta & clean output to run AlphaFold2 job
        """
        protein_structure_id = kwargs['dag_run'].conf["protein_structure_id"]
        task_type = kwargs['dag_run'].conf["task_type"]
        if str(task_type) == 'STRUCTURE_PREDICTION_MULTIMER':
            protein_structure_id = str(protein_structure_id) + '_multimer'
        enable_predict = kwargs['dag_run'].conf.get("enable_predict", False)
        if enable_predict:
            oss = OssClient(bucket_name="alphafold-hangzhou")
            if is_step1_finished(protein_structure_id, oss):
                # alphafold step1 calculation is done, proceed from step2
                remote_fasta = generate_remote_fasta_for_step2(protein_structure_id)
                clear_keys_for_step2(oss, protein_structure_id)
                oss.put_object_from_file(key=remote_fasta, local_path=fasta_file)
            else:
                # alphafold step1 calculation is not done, start from step1
                remote_fasta = generate_remote_fasta(protein_structure_id)
                clear_keys(oss, protein_structure_id)
                oss.put_object_from_file(key=remote_fasta, local_path=fasta_file)
            if os.path.exists(fasta_file):
                os.remove(fasta_file)

    @task(retry_delay=timedelta(minutes=10), retries=30, pool="protein_predict_job_pool")
    def check_protein_predict_job(**kwargs):
        from mixbio.common.retry_exception import AirflowRetryException
        from mixbio.common.oss_client import OssClient

        """
        may wait several hours, should not blocking workers
        """
        enable_predict = kwargs['dag_run'].conf.get("enable_predict", False)
        protein_structure_id = kwargs['dag_run'].conf["protein_structure_id"]
        task_type = kwargs['dag_run'].conf["task_type"]
        if str(task_type) == 'STRUCTURE_PREDICTION_MULTIMER':
            protein_structure_id = str(protein_structure_id) + '_multimer'
        if enable_predict:
            alphafold_bucket = "alphafold-hangzhou"
            oss = OssClient(bucket_name=alphafold_bucket)

            pdb_status = generate_remote_pdb_status(protein_structure_id)
            if oss.object_exists(pdb_status):
                status_result = oss.get_object_body(pdb_status)
                logging.info("Get oss job status,pdb:{} status:{}".format(pdb_status, status_result))
                if "success" in status_result:
                    default_oss = OssClient()
                    remote_pdb = generate_remote_pdb(protein_structure_id)
                    default_pdb_key = generate_default_oss_remote_pdb(protein_structure_id)
                    if oss.object_exists(remote_pdb) and (oss.object_size(remote_pdb) > 0):
                        default_oss.copy_object(alphafold_bucket, remote_pdb, default_pdb_key)
                    else:
                        logging.error("Get oss job result,remote pdb:{} not exist".format(remote_pdb))
                        raise AirflowFailException("The protein job error,mark task fail not retry")
                elif "error" in status_result:
                    raise AirflowFailException("The protein job error,mark task fail not retry")
                else:
                    raise AirflowRetryException('AlphaFold job not success,mark task error to retry')
            else:
                logging.warning("Get oss job status,pdb:{} not exist".format(pdb_status))
                raise AirflowRetryException('AlphaFold job not success,mark task error to retry')

    @task(trigger_rule="none_failed_min_one_success", retry_delay=timedelta(minutes=5), retries=10)
    def use_external_pdb(**kwargs):
        from mixbio.common.retry_exception import AirflowRetryException
        from mixbio.common.oss_client import OssClient
        protein_structure_id = kwargs['dag_run'].conf["protein_structure_id"]
        task_type = kwargs['dag_run'].conf["task_type"]
        if str(task_type) == 'STRUCTURE_PREDICTION_MULTIMER':
            protein_structure_id = str(protein_structure_id) + '_multimer'
        kwargs['dag_run'].conf["structureSource"] = "EXTERNAL"
        context = get_current_context()
        pdb = context['ti'].xcom_pull(
            task_ids='check_protein_exist', key='key_external_pdb')
        external_pdb_key = "alphafold_ext/%s/%s" % (str(protein_structure_id), pdb['pdb_name'])
        fp = protein_external_api.download_pdb(pdb['url'])
        if fp is None:
            print(f"pdb downloaded fail:{pdb['url']}")
            raise AirflowRetryException('download file fail')
        else:
            print(f"pdb downloaded:{fp.name} from:{pdb['url']}")
            oss = OssClient()
            oss.put_object_from_file(key=external_pdb_key, local_path=fp.name)
            os.remove(fp.name)
        crispr_server_endpoint = Variable.get("crispr_server_endpoint", default_var="localhost")
        conf = kwargs['dag_run'].conf
        json_params = {
            'dagRunId': kwargs['dag_run'].run_id,
            "platformJobId": conf["platform_job_id"],
            "proteinTaskId": conf["protein_task_id"],
            "proteinStructureId": conf["protein_structure_id"],
            "jobStatus": "SUCCESS",
            "pdbStorageKey": external_pdb_key,
            "structureSource": conf.get("structureSource", None),
            "taskType": conf["task_type"]
        }
        response = requests.post(crispr_server_endpoint + "/api/v1/protein/notify/predictResult", json=json_params,
                                 timeout=120)
        logging.info("request json:{}".format(json))
        check_http_response(response)

    @task(trigger_rule="none_failed_min_one_success")
    def notify_pdb_success(**kwargs):
        conf = kwargs['dag_run'].conf
        dag_run_id = kwargs['dag_run'].run_id
        send_predict_result_by_http_request("SUCCESS", conf, dag_run_id)

    @task(trigger_rule="one_failed")
    def notify_pdb_fail(**kwargs):
        conf = kwargs['dag_run'].conf
        dag_run_id = kwargs['dag_run'].run_id
        send_predict_result_by_http_request("FAILED", conf, dag_run_id)

    def send_predict_result_by_http_request(job_status: str, conf, dag_run_id):
        enable_predict = conf.get("enable_predict", False)
        protein_structure_id = conf["protein_structure_id"]
        task_type = conf["task_type"]
        if str(task_type) == 'STRUCTURE_PREDICTION_MULTIMER':
            protein_structure_id = str(protein_structure_id) + '_multimer'
        if enable_predict:
            pdb_key = generate_default_oss_remote_pdb(protein_structure_id)
        else:
            pdb_key = "pdb_demo/4oo8.pdb"

        """
        notify pdb result
        """
        crispr_server_endpoint = Variable.get("crispr_server_endpoint", default_var="localhost")
        json = {
            'dagRunId': dag_run_id,
            "platformJobId": conf["platform_job_id"],
            "proteinTaskId": conf["protein_task_id"],
            "proteinStructureId": conf["protein_structure_id"],
            "jobStatus": job_status,
            "pdbStorageKey": pdb_key,
            "structureSource": conf.get("structureSource", None),
            "taskType": conf["task_type"]
        }
        response = requests.post(crispr_server_endpoint + "/api/v1/protein/notify/predictResult", json=json,
                                 timeout=120)
        logging.info("request json:{}".format(json))
        check_http_response(response)

    def check_http_response(response):
        from mixbio.common.retry_exception import AirflowRetryException

        logging.info("Call http response, status:{} body: {}".format(response.status_code, response.text))
        if 400 <= response.status_code < 500:
            raise AirflowFailException("Http request bad,mark failed without retrying")
        elif 500 <= response.status_code < 600:
            raise AirflowRetryException("Http request error,mark task error to retry")

    def check_protein_exist_branch(ti, **kwargs):
        from mixbio.common.oss_client import OssClient

        # print("max_runs: ", max_runs)

        # look for external pdb
        fasta_sequence = kwargs['dag_run'].conf["fasta_sequence"]
        external_pdb = protein_external_api.check_pdb_exist_by_sequence(fasta_sequence.upper())
        if external_pdb['pdb']:
            ti.xcom_push(key='key_external_pdb', value=external_pdb)
            return ["use_external_pdb"]

        #  check if predicted
        protein_structure_id = kwargs['dag_run'].conf["protein_structure_id"]
        task_type = kwargs['dag_run'].conf["task_type"]
        if str(task_type) == 'STRUCTURE_PREDICTION_MULTIMER':
            protein_structure_id = str(protein_structure_id) + '_multimer'
        default_pdb_key = generate_default_oss_remote_pdb(protein_structure_id)
        default_oss = OssClient()

        if default_oss.object_exists(default_pdb_key):
            logging.info("Object exists, default_pdb_key:{}".format(default_pdb_key))
            return ["notify_pdb_success"]
        else:
            # check if predicted
            alphafold_bucket = "alphafold-hangzhou"
            alphafold_oss = OssClient(bucket_name=alphafold_bucket)
            remote_pdb = generate_remote_pdb(protein_structure_id)

            # if exists in the bucket of alphaFold, copy it to the crispr bucket
            if alphafold_oss.object_exists(remote_pdb) and (alphafold_oss.object_size(remote_pdb) > 0):
                logging.info("Object exists, remote_pdb:{}".format(default_pdb_key))
                default_oss.copy_object(alphafold_bucket, remote_pdb, default_pdb_key)
                return ["notify_pdb_success"]
            else:
                return ["generate_fasta"]

    def generate_default_oss_remote_pdb(protein_structure_id):
        return "alphafold_output/%s/ranked_0.pdb" % str(protein_structure_id)

    def generate_remote_pdb(protein_structure_id):
        env = Variable.get("env", default_var="dev")

        return "%s/output/alphafold/%s/ranked_0.pdb" % (env, str(protein_structure_id))

    def generate_remote_pdb_status(protein_structure_id):
        env = Variable.get("env", default_var="dev")

        return "%s/output/alphafold/%s/status" % (env, str(protein_structure_id))

    # check if alphafold step1 calculation is done, to avoid repeated step1 calculation
    def is_step1_finished(protein_structure_id, oss):
        env = Variable.get("env", default_var="dev")
        # features.pkl  --> step1 is done
        feature_file_key = "%s/output/alphafold/%s/features.pkl" % (env, str(protein_structure_id))
        return oss.object_exists(feature_file_key) and (oss.object_size(feature_file_key) > 0)

    def generate_remote_fasta(protein_structure_id):
        env = Variable.get("env", default_var="dev")
        return "%s/input/alphafold/%s.fasta" % (env, str(protein_structure_id))

    def generate_remote_fasta_for_step2(protein_structure_id):
        env = Variable.get("env", default_var="dev")
        return "%s/middle/input/alphafold/%s.fasta" % (env, str(protein_structure_id))

    def clear_keys(oss, protein_structure_id):
        env = Variable.get("env", default_var="dev")

        oss.delete_objects_by_prefix("%s/middle/output/alphafold/%s" % (env, str(protein_structure_id)))
        oss.delete_objects_by_prefix("%s/output/alphafold/%s" % (env, str(protein_structure_id)))

    def clear_keys_for_step2(oss, protein_structure_id):
        env = Variable.get("env", default_var="dev")
        oss.delete_objects_by_prefix("%s/output/alphafold/%s/lock" % (env, str(protein_structure_id)))
        keys = ["%s/output/alphafold/%s/status" % (env, str(protein_structure_id)),
                "%s/output/alphafold/%s/version" % (env, str(protein_structure_id))]
        oss.delete_objects(keys)

    check_protein_exist = BranchPythonOperator(
        task_id="check_protein_exist",
        python_callable=check_protein_exist_branch,
    )

    generate_fasta_op = generate_fasta()
    protein_predict_op = trigger_protein_predict_job(generate_fasta_op)
    check_predict_op = check_protein_predict_job()

    notify_pdb_success_op = notify_pdb_success()
    notify_pdb_fail_op = notify_pdb_fail()
    use_external_pdb_op = use_external_pdb()

    chain(check_protein_exist, notify_pdb_success_op)
    chain(check_protein_exist, use_external_pdb_op, notify_pdb_fail_op)
    chain(check_protein_exist, generate_fasta_op, protein_predict_op, check_predict_op,
          [notify_pdb_success_op, notify_pdb_fail_op])


dag = protein_predict()
