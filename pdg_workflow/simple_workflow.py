# unparallelized PDG workflow

# filepaths
from pathlib import Path
import os

# visual checks & vector data wrangling
import geopandas as gpd

# staging
import pdgstaging
from pdgstaging import TileStager

# rasterization & web-tiling
import pdgraster
from pdgraster import RasterTiler

import shutil

# for transferring the log to workdir
import subprocess
from subprocess import Popen

print("Removing old directories and files...")
old_filepaths = ["staging_summary.csv",
                 "raster_summary.csv",
                 "raster_events.csv",
                 "config__updated*",
                 "log.log"]
for old_file in old_filepaths:
  if os.path.exists(old_file):
      os.remove(old_file)

old_dirs = ["staged",
            "geotiff",
            "web_tiles"]
for old_dir in old_dirs:
  if os.path.exists(old_dir) and os.path.isdir(old_dir):
      shutil.rmtree(old_dir)


workflow_config = { 
  "dir_input": "/home/jcohen/testing/testing_datasets/iwp_2_files", 
  "ext_input": ".gpkg",
  "dir_staged": "staged/",
  "dir_geotiff": "geotiff/", 
  "dir_web_tiles": "web_tiles/", 
  "filename_staging_summary": "staging_summary.csv",
  "filename_rasterization_events": "raster_events.csv",
  "filename_rasters_summary": "raster_summary.csv",
  "filename_config": "config",
  "simplify_tolerance": 0.1,
  "tms_id": "WGS1984Quad",
  "z_range": [
    0,
    10
  ],
  "geometricError": 57,
  "z_coord": 0,
  "statistics": [
    {
      "name": "iwp_coverage",
      "weight_by": "area",
      "property": "area_per_pixel_area",
      "aggregation_method": "sum",
      "resampling_method": "average",
      "val_range": [
        0,
        1
      ],
      "palette": [
        "#66339952",
        "#ffcc00"
      ],
      "nodata_val": 0,
      "nodata_color": "#ffffff00"
    },
  ],
  "deduplicate_clip_to_footprint": False,
  "deduplicate_at": None,
  "deduplicate_method": None
}


def run_pdg_workflow(workflow_config):
    """
    Run the main PDG workflow for the following steps:
    1. staging
    2. raster highest
    3. raster lower
    4. web tiling

    Parameters
    ----------
    workflow_config : dict
        Configuration for the PDG staging workflow.
    """

    # stage the tiles
    stager = TileStager(workflow_config)
    stager.stage_all()

    # rasterize all staged tiles, resample to lower resolutions,
    # and produce web tiles
    RasterTiler(workflow_config).rasterize_all()
   
if __name__ == "__main__":
    run_pdg_workflow(workflow_config)

print("Script complete.")

