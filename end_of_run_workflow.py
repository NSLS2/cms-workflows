import os
from prefect import flow, get_run_logger, task
from prefect.task_runners import ConcurrentTaskRunner

#from analysis import run_analysis
from data_validation import read_all_streams, data_validation_task
from linker import create_symlinks
from dotenv import load_dotenv


def get_api_key_from_env():
    with open("/srv/container.secret", "r") as secrets:
        load_dotenv(stream=secrets)
    api_key = os.environ["TILED_API_KEY"]
    return api_key


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow(task_runner=ConcurrentTaskRunner())
def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
    logger = get_run_logger()
    uid = stop_doc["run_start"]
    if not api_key:
        api_key = get_api_key_from_env()

    # Launch validation, analysis, and linker tasks concurrently
    linker_task = create_symlinks.submit(uid, api_key=api_key, dry_run=dry_run)
    logger.info("Launched linker task")

    read_streams_task = read_all_streams.submit(uid, api_key=api_key)
    validation_task = data_validation_task.submit(uid, api_key=api_key)
    logger.info("Launched validation tasks")

    # analysis_task = run_analysis(raw_ref=uid)
    # logger.info("Launched analysis task")

    # Wait for all tasks to comple
    logger.info("Waiting for tasks to complete")
    linker_task.result()
    read_streams_task.result()
    validation_task.result()
    # analysis_task.result()
    log_completion()
