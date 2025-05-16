from prefect import flow, get_run_logger, task
from prefect.task_runners import ConcurrentTaskRunner

#from analysis import run_analysis
from data_validation import read_all_streams
from linker import create_symlinks


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow(task_runner=ConcurrentTaskRunner())
def end_of_run_workflow(stop_doc):
    logger = get_run_logger()
    uid = stop_doc["run_start"]

    # Launch validation, analysis, and linker tasks concurrently
    linker_task = create_symlinks.submit(uid)
    logger.info("Launched linker task")

    validation_task = read_all_streams.submit(uid, beamline_acronym="cms")
    logger.info("Launched validation task")

    # analysis_task = run_analysis(raw_ref=uid)
    # logger.info("Launched analysis task")

    # Wait for all tasks to comple
    logger.info("Waiting for tasks to complete")
    linker_task.result()
    validation_task.result()
    # analysis_task.result()
    log_completion()
