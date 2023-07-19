import os
from shutil import copy2, copytree

from pathlib import Path
import event_model

from prefect import flow, get_run_logger, task
from tiled.client import from_profile


tiled_client = from_profile("nsls2", username=None)["cms"]
tiled_client_raw = tiled_client["raw"]


@task
def copy_file(source, dest_dir):
    logger = get_run_logger()
    # print(f"Copying {source} to {dest_dir}")
    logger.info(f"Copying {source} to {dest_dir}")
    copy2(source, dest_dir)

    # print("Done copying file")
    logger.info("Done copying files")


# @task
# def copy_tree(source, dest_dir):
#     logger = get_run_logger()
#     # print(f"Copying files from {source} to {dest_dir}")
#     logger.info(f"Copying files from {source} to {dest_dir}")
#     # copytree(source, dest_dir, dirs_exist_ok=True)
#     # print("Done copying files")
#     logger.info("Done copying files")


def get_det_file_paths(run):
    docs = run.documents()
    root_map = {}
    target_keys = set()
    resource_info = {}
    datum_info = {}
    source_paths = []

    for name, doc in docs:
        if name == "start":
            pass

        elif name == "resource":
            # we only handle AD TIFF
            if doc["spec"] != "AD_TIFF":
                continue
            doc_root = doc["root"]
            resource_info[doc["uid"]] = {
                "path": Path(root_map.get(doc_root, doc_root)) / doc["resource_path"],
                "kwargs": doc["resource_kwargs"],
            }
            pass
        elif "datum" in name:
            if name == "datum":
                doc = event_model.pack_datum_page(doc)

            for datum_uid, point_number in zip(
                doc["datum_id"], doc["datum_kwargs"]["point_number"]
            ):
                datum_info[datum_uid] = (
                    resource_info[doc["resource"]],
                    point_number,
                )
        elif name == "descriptor":
            for k, v in doc["data_keys"].items():
                if "external" in v:
                    target_keys.add(k)
        elif "event" in name:
            if name == "event":
                doc = event_model.pack_event_page(doc)
            for key in target_keys:
                if key not in doc["data"]:
                    continue
                for datum_id in doc["data"][key]:
                    resource_vals, point_number = datum_info[datum_id]
                    orig_template = resource_vals["kwargs"]["template"]
                    fpp = resource_vals["kwargs"]["frame_per_point"]

                    base_fname = resource_vals["kwargs"]["filename"]
                    for fr in range(fpp):
                        source_path = Path(
                            orig_template
                            % (
                                str(resource_vals["path"]) + "/",
                                base_fname,
                                point_number * fpp + fr,
                            )
                        )
                        source_paths.append(source_path)
    return source_paths


def get_data_filename(detector, dest_dir, savename, subdirs=True):
    logger = get_run_logger()
    subdir = ''
    if subdirs:
        if detector == 'pilatus300' or  detector == 'pilatus8002' :
            subdir = 'maxs/raw/'
            detname = 'maxs'
        elif detector ==  'pilatus2M':
            subdir = 'saxs/raw/'
            detname = 'saxs'
        elif detector ==  'pilatus800':
            subdir = 'waxs/raw/'
            detname = 'waxs'
        else:
            # print(f"WARNING: Can't do file handling for detector '{detector}'.")
            logger.warn(f"WARNING: Can't do file handling for detector '{detector}'.")
            return

    # Create subdirs
    Path(f"{dest_dir}{subdir}").mkdir(parents=True, exist_ok=True)
    file_name = f"{dest_dir}{subdir}{savename}_{detname}.tiff"
    return file_name


@flow
def export(ref, subdirs=True):
    logger = get_run_logger()
    run = tiled_client_raw[ref]
    full_uid = run.start["uid"]
    # print(f"{full_uid = }")
    logger.info(f"{full_uid = }")
    cycle = run.start.get("experiment_cycle").replace("_", "-")
    # print(f"{cycle = }")
    logger.info(f"{cycle = }")
    proposal_num = run.start.get("experiment_proposal_number")
    # print(f"{proposal_num = }")
    logger.info(f"{proposal_num = }")
    # exp_alias_dir = run.start["experiment_alias_directory"]
    # dest_dir = exp_alias_dir
    dest_dir = f"/nsls2/data/cms/proposals/{cycle}/pass-{proposal_num}/"
    if not os.path.exists(dest_dir):
        logger.info(f"Directory {dest_dir} doesn't exist. Not copying files for {full_uid}.")
        return
    dets = run.start.get("detectors")
    savename = run.start.get("filename")
    if savename is None:
        logger.info(f"Couldn't get 'savename'. Not copying files for {full_uid}.")
        return

    resource_paths = get_det_file_paths(run)
    for i, det in enumerate(dets):
        source = str(resource_paths[i])
        if not os.path.exists(source):
            logger.info(f"{source} doesn't exist. Not copying files for {full_uid}.")
            return
        dest = get_data_filename(det, dest_dir, savename, subdirs=subdirs)
        copy_file(source, dest)
