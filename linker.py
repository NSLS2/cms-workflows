from prefect import task, get_run_logger
from pathlib import Path
from utils import get_tiled_client
import os
import glob

#tiled_client = from_uri('https://tiled.nsls2.bnl.gov')
tiled_client = get_tiled_client()
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

def chmod_and_chown(path, *, uid=None, gid=None, mode=0o775):
    os.chmod(path, mode)

    # The following needs to be tested more; Prefect worker account doesn't have permissions to chown
    #if uid is not None and gid is not None:
    #    os.chown(path, uid, gid)

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
                logger.info("Skipping the creation of the link because 'experiment_project' is set.")
                return
            detectors = doc.get("detectors", [])
            if filename := doc.get("filename"):
                pass
            else:
                logger.info("Skipping the creation of the link because 'filename' is not set.")
                return
            if path_expr_alias := doc.get("experiment_alias_directory"):
                path_proposal = Path(f"/nsls2/data/cms/proposals/{doc['cycle']}/{doc['data_session']}")
                stats = path_proposal.stat()
                path_expr = path_proposal / "experiments"   # experiments directory
                path_expr.mkdir(exist_ok=True, parents=True)
                chmod_and_chown(path_expr, uid=stats.st_uid, gid=stats.st_gid)
                path_expr_alias = path_expr / path_expr_alias
                path_expr_alias.mkdir(exist_ok=True, parents=True)
                chmod_and_chown(path_expr_alias, uid=stats.st_uid, gid=stats.st_gid)
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
                        path_analysis = Path(path_expr_alias) / subdir_analysis
                        path_analysis.mkdir(exist_ok=True, parents=True)
                        chmod_and_chown(path_analysis, uid=stats.st_uid, gid=stats.st_gid)
                        chmod_and_chown(path_analysis.parent, uid=stats.st_uid, gid=stats.st_gid)
                        path_data = Path(path_expr_alias) / 'data'
                        path_data.mkdir(exist_ok=True, parents=True)
                        chmod_and_chown(path_data, uid=stats.st_uid, gid=stats.st_gid)
                        logger.info(f"Created analysis and data folders for {det}")
    
                        prefix = str(Path(doc["root"]) / doc["resource_path"] / doc["resource_kwargs"]["filename"])
                        for file_path in glob.glob(prefix + "*"):
                            source_name = os.path.splitext(os.path.basename(file_path))[0]  # only file name w/o extension
                            name, indx = source_name.split("_")    # filename and index of the image
                            link_path = Path(path_expr_alias) / subdir_raw / f"{filename or name}_{indx}_{detname}.tiff"
                            link_path.parent.mkdir(exist_ok=True, parents=True)
                            chmod_and_chown(link_path.parent, uid=stats.st_uid, gid=stats.st_gid)
                            os.symlink(file_path, link_path)
                            logger.info(f"Linked: {file_path} to {link_path}")
                        break
            else:
                logger.error(f"Resource document referencing unknown detector {det}.")
