#from tiled.client import from_uri
#import logging
from prefect import task, get_run_logger
from pathlib import Path
from tiled.client import from_profile
import os
import glob

#tiled_client = from_uri('https://tiled.nsls2.bnl.gov')
tiled_client = from_profile("nsls2")['cms']
tiled_client_raw = tiled_client["raw"]

#logger = logging.getLogger()


def detector_mapping(detector):
    if detector in {"pilatus300k-1", "pilatus800k-2"}:
        return "maxs"
    elif detector == "pilatus2m-1":
        return "saxs"
    elif detector == "pilatus800k-1":
        return "waxs"
    elif "webcam" in detector:
        return detector
    else:
        return None

@task
def create_symlinks(ref):
    """
    Parameters
    ----------
    ref : Union[int, str]
        Scan_id or uid of the start document

    """
    logger = get_run_logger()

    hrf = tiled_client_raw[ref]
    for name, doc in hrf.documents():
        if name == "start":
            if doc.get('experiment_project'):
                # NOTE: shortcut for the workflow before data security; to be removed later
                return
            detectors = doc.get("detectors", [])
            if filename := doc.get("filename"):
                pass
            else:
                logger.info("Skipping the creation of the link because 'filename' is not set.")
                return
            if link_root := doc.get("experiment_alias_directory"):
                link_root = f"/nsls2/data/cms/proposals/{doc['cycle']}/{doc['data_session']}/experiments/"+link_root
            else:
                logger.info("Directory for links is not specified; skipping.")
                return

        elif name == "resource":
            for det in detectors:
                if det in doc["root"]:
                    if detname := detector_mapping(det):
                        # Define subfolders for "raw" and "analysis", but not for cameras
                        subdir_raw = "camera" if "webcam" in detname else f"{detname}/raw"
                        subdir_analysis = "camera" if "webcam" in detname else f"{detname}/analysis"
                        subdir_data = 'data'
    
                        prefix = str(Path(doc["root"]) / doc["resource_path"] / doc["resource_kwargs"]["filename"])
                        for file_path in glob.glob(prefix + "*"):
                            source_name = os.path.splitext(os.path.basename(file_path))[0]  # only file name w/o extension
                            name, indx = source_name.split("_")    # filename and index of the image
                            link_path = Path(link_root) / subdir_raw / f"{filename or name}_{indx}_{detname}.tiff"
                            link_path.parent.mkdir(exist_ok=True, parents=True)
                            os.symlink(file_path, link_path)
                            logger.info(f"Linked: {file_path} to {link_path}")

                            (Path(link_root) / subdir_analysis).mkdir(exist_ok=True, parents=True)
                            (Path(link_root) / subdir_data).mkdir(exist_ok=True, parents=True)
                            
                            logger.info(f"Created analysis folder for {det}")
    
                        break
            else:
                logger.error(f"Resource document referencing unknown detector {det}.")
