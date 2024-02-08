# test docker image and container by running a
# minimum version of the workflow

from datetime import datetime
import json
import logging
import logging.handlers
import os

import pdgstaging
from pdgstaging import logging_config
import pdgraster

import shutil


#base_dir = "/home/jcohen/docker_python_basics/app-data/"
base_dir = "/app/"
# TOD0: define base dir by pulling the WORKDIR from Dockerfile
# or pulling the second component (filepath) from the parsl config's "persistent_volumes" definition
# instead of hard-coding

# start with a fresh directory!
print("Removing old directories and files...")
old_filepaths = [f"{base_dir}staging_summary.csv",
                f"{base_dir}raster_summary.csv",
                f"{base_dir}raster_events.csv",
                f"{base_dir}config__updated.json",
                f"{base_dir}log.log"]
for old_file in old_filepaths:
  if os.path.exists(old_file):
      os.remove(old_file)

# remove dirs from past run
old_dirs = [f"{base_dir}staged",
            f"{base_dir}geotiff",
            f"{base_dir}web_tiles"]
for old_dir in old_dirs:
  if os.path.exists(old_dir) and os.path.isdir(old_dir):
      shutil.rmtree(old_dir)


#lc = "/home/jcohen/docker_python_basics/data/test_polygons.gpkg"

config = {
  "deduplicate_clip_to_footprint": False,
  "deduplicate_method": None,
  "dir_output": base_dir, 
  "dir_input": "/home/jcohen/docker_python_basics/data", 
  "ext_input": ".gpkg",
  "dir_staged": base_dir+"staged/", 
  "dir_geotiff": base_dir+"geotiff/",  
  "dir_web_tiles": base_dir+"web_tiles/", 
  "filename_staging_summary": base_dir+"staging_summary.csv",
  "filename_rasterization_events": base_dir+"raster_events.csv",
  "filename_rasters_summary": base_dir+"raster_summary.csv",
  "version": datetime.now().strftime("%B%d,%Y"),
  "simplify_tolerance": 0.1,
  "tms_id": "WGS1984Quad",
  "z_range": [
    0,
    7
  ],
  "geometricError": 57,
  "z_coord": 0,
  "statistics": [
    {
      "name": "change_rate", 
      "weight_by": "area",
      "property": "ChangeRateNet_myr-1", 
      "aggregation_method": "min", 
      "resampling_method": "mode",  
      "val_range": [
        -2,
        2
      ],
      "palette": ["#ff0000", # red
                  "#FF8C00", # DarkOrange
                  "#FFA07A", # LightSalmon
                  "#FFFF00", # yellow
                  "#66CDAA", # MediumAquaMarine
                  "#AFEEEE", # PaleTurquoise,
                  "#0000ff"], # blue
      "nodata_val": 0,
      "nodata_color": "#ffffff00" # fully transparent white
    },
  ],
}

print("Staging...")
stager = pdgstaging.TileStager(config = config, check_footprints = False)
# generate the staged files
stager.stage("test_polygons.gpkg")

print("Staging complete. Rasterizing...")

# for initial testing, only rasterize the highest z-level:
# staged_paths = stager.tiles.get_filenames_from_dir(base_dir = "staged")
# rasterizer = pdgraster.RasterTiler(config)
# rasterizer.rasterize_vectors(staged_paths, make_parents = False)

# or rasterize all z-levels and web tiles:
rasterizer = pdgraster.RasterTiler(config)
rasterizer.rasterize_all()

print("Script complete.")