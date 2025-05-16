from prefect import task, get_run_logger
from pathlib import Path
from tiled.client import from_profile
import os
import glob

tiled_client = from_profile("nsls2")["smi"]
tiled_client_raw = tiled_client["raw"]

logger = get_run_logger()


def detector_mapping(detector):
    if detector in {"pilatus300k-1", "pilatus800k-2"}:
        subdir = "maxs/raw"
        detname = "maxs"
    elif detector == "pilatus2m-1":
        subdir = "saxs/raw"
        detname = "saxs"
    elif detector == "pilatus800k-1":
        subdir = "waxs/raw"
        detname = "waxs"
    else:
        logger.error("WARNING: Can't do file handling for detector '{}'.".format(detector))
        return None, None
    
    return subdir, detname

@task
def create_symlinks(ref):
    """
    Parameters
    ----------
    ref : Union[int, str]
        Scan_id or uid of the start document

    Returns
    -------
    list[tuple[str, Path, Path]]
         A tuple of the start uid, the source path and the destination path.
    """
    logger = get_run_logger()

    hrf = tiled_client_raw[ref]
    for name, doc in hrf.documents():
        if name == "start":
            detectors = doc.get("detectors", [])
            link_root = doc.get("experiment_alias_directory", None)

        elif name == "resource":
            for det in detectors:
                if det in doc["root"]:
                    subdir, detname = detector_mapping(det)
                    prefix = str(Path(doc["root"]) / doc["resource_path"] / doc["resource_kwargs"]["filename"])

                    for file_name in glob.glob(prefix + "*"):
                        savename = os.path.splitext(os.path.basename(file_name))[0]  # only file name w/o extension
                        link_name = Path(link_root) / subdir / f"{savename}_{detname}.tiff"
                        os.symlink(file_name, link_name)
                        logger.info(f"Linked: {file_name} to {link_name}")

                    break
            else:
                logger.error(f"Resource document referencing unknown detector {det}.")
