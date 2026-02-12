import time

from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret

from bluesky_tiled_plugins.writing.validator import validate
from tiled.client import from_profile


@task(retries=2, retry_delay_seconds=10)
def read_all_streams(beamline_acronym, uid):
    logger = get_run_logger()
    api_key = Secret.load("tiled-cms-api-key", _sync=True).get()
    tiled_client = from_profile("nsls2", api_key=api_key)
    run = tiled_client[beamline_acronym]["raw"][uid]
    logger.info(f"Validating uid {run.start['uid']}")
    start_time = time.monotonic()
    for stream in run:
        logger.info(f"{stream}:")
        stream_start_time = time.monotonic()
        stream_data = run[stream].read()
        stream_elapsed_time = time.monotonic() - stream_start_time
        logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
        logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = time.monotonic() - start_time
    logger.info(f"{elapsed_time = }")


@task(retries=3, retry_delay_seconds=20)
def data_validation_task(uid, beamline_acronym="cms"):
    """Task to validate the data structure and accessibility in Tiled

    Parameters
    ----------
        uid : str
            The UID of the run to validate
        beamline_acronym : str, optional
            The acronym of the beamline (default is "cms")
    """

    logger = get_run_logger()

    api_key = Secret.load(f"tiled-{beamline_acronym}-api-key", _sync=True).get()
    tiled_client = from_profile("nsls2", api_key=api_key)

    logger.info(f"Connecting to Tiled client for beamline '{beamline_acronym}'")
    run_client = tiled_client[f"{beamline_acronym}/migration"][uid]

    logger.info(f"Validating uid {uid}")
    start_time = time.monotonic()
    validate(run_client, fix_errors=True, try_reading=True, raise_on_error=True)
    elapsed_time = time.monotonic() - start_time
    logger.info(f"Finished validating data; {elapsed_time = }")


@flow(log_prints=True)
def data_validation_flow(uid):
    data_validation_task(uid)
