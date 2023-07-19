from prefect import flow, get_run_logger, task

from analysis import analysis_flow
from data_validation import general_data_validation
from export import export


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow
def end_of_run_workflow(stop_doc):
    uid = stop_doc["run_start"]

    # return_state=True allows the export flow to run even if the
    # data validation fails
    general_data_validation(beamline_acronym="cms", uid=uid, return_state=True)
    export(ref=uid, subdirs=True)

    # analysis_flow(raw_ref=uid)
