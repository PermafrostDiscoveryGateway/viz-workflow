# testing addition to rasterize_vectors where file is tried again before gives up
# using the subset data sample from ingmar with additional rasters that will certainly fail

# 1000 polygons of the input files
# and z-levels 1-11 rather than 1-16

# file paths
import os
from pathlib import Path

# visualization
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import box

# PDG packages
import pdgstaging
import pdgraster
#import py3dtiles
#import viz_3dtiles
#from viz_3dtiles import TreeGenerator, BoundingVolumeRegion
#import pdgpy3dtiles
#from StagedTo3DConverter import StagedTo3DConverter

# logging and configuration
from datetime import datetime
import logging
import logging.config
import argparse
import json

# Parsl
import parsl
from parsl import python_app
from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider

# to corrupt 1 gpkg file
import sqlite3

# use sample of 1000 random polygons from each of the 3 files from Ingmar
base_dir = Path('/home/jcohen/viz-workflow/raster-retry/data_subsample')
filename = 'lake_change_data.gpkg'
data_paths = [p.as_posix() for p in base_dir.glob('**/' + filename)]

workflow_config = '/home/jcohen/viz-workflow/raster-retry/ingmar_config.json'

# logging setup
logging_config = '/home/jcohen/viz-workflow/raster-retry/logging.json'

def setup_logging(log_json_file):
    """
    Setup logging configuration
    """
    with open(log_json_file, 'r') as f:
        logging_dict = json.load(f)
    logging.config.dictConfig(logging_dict)
    return logging_dict

logging_dict = setup_logging(logging_config)

logger = logging.getLogger(__name__)

start_time = datetime.now()

# define managers
stager = pdgstaging.TileStager(workflow_config)
tile_manager = stager.tiles
config_manager = stager.config
rasterizer = pdgraster.RasterTiler(workflow_config)
# zoom levels configuration
min_z = config_manager.get_min_z()
max_z = config_manager.get_max_z()
parent_zs = range(max_z - 1, min_z - 1, -1)

# set up parsl provider
# bash command to activate virtual environment
activate_env = 'source /home/jcohen/.bashrc; conda activate rasterRetry'

htex_config_local = Config(
  executors = [
      HighThroughputExecutor(
        label = "htex_Local",
        cores_per_worker = 2, 
        max_workers = 2, # why would this be so low? because just testing with small amount of data ?
          # worker_logdir_root = '/' only necessary if the file system is remote, which is not the case for this lake change sample
          # address not necessary because we are not using kubernetes
        worker_debug = False, # don't need this because we have logging setup
          # provider is local for this run thru, kubernetes would use KubernetesProvider()
        provider = LocalProvider(
          channel = LocalChannel(),
          worker_init = activate_env,
          init_blocks = 1, # default I think
          max_blocks = 10 # changed from deafult of 1
        ),
      )
    ],
  )

parsl.clear() # first clear the current configuration since we will likely run this script multiple times
parsl.load(htex_config_local) # load the config we just outlined

# set batch size for rasterization & web tiling
batch_size_staging=1
batch_size_rasterization=30
batch_size_web_tiles=200
batch_size_geotiffs=200

def make_batch(items, batch_size):
    """
    Create batches of a given size from a list of items.
    """
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

input_batches = make_batch(data_paths, batch_size_staging)
input_batches # 3 batches, 1 file each

# STAGING
@python_app
def stage(paths, config, logging_dict = logging_dict): 
    """
    Stage files (step 1)
    """
    import pdgstaging
    if logging_dict:
        import logging.config
        logging.config.dictConfig(logging_dict)
    stager = pdgstaging.TileStager(config)
    for path in paths:
        stager.stage(path)
    return True

app_futures = []
for batch in input_batches:
    app_future = stage(batch, workflow_config, logging_dict)
    app_futures.append(app_future)

[a.result() for a in app_futures]

logger.info('staging done! Moving on to corrupt 1 gpkg file.')

# CORRUPT 1 GPKG FILE
# create database connection to one staged file
conn = sqlite3.connect('/home/jcohen/viz-workflow/raster-retry/OUTPUT_STAGING_TILES/WGS1984Quad/11/408/244.gpkg')
# define database cursor to fetch results from SQL queries
cur = conn.cursor()
# delete data from ref_sys_table
cur.execute("""
DROP TABLE gpkg_spatial_ref_sys;
""")
# save the change to the database
conn.commit()
# close connections
cur.close()
conn.close()

logger.info('Corrupted gpkg file 11/408/244.gpkg by dropping the entire table gpkg_spatial_ref_sys.')

# RASTERIZATION FOR HIGHEST Z-LEVEL
staged_paths = stager.tiles.get_filenames_from_dir('staged')
staged_batches = make_batch(staged_paths, batch_size_rasterization)

@python_app
def rasterize(staged_paths, config, logging_dict = logging_dict):
    """
    Rasterize a batch of vector files (step 2) for highest z-level only
    """
    import pdgraster
    if logging_dict:
        import logging.config
        logging.config.dictConfig(logging_dict)
    rasterizer = pdgraster.RasterTiler(config)
    return rasterizer.rasterize_vectors(staged_paths, make_parents = False)

app_futures = []
for batch in staged_batches:
    app_future = rasterize(batch, workflow_config, logging_dict)
    app_futures.append(app_future)

[a.result() for a in app_futures]

logger.info('rasterization for highest z-level done!')

# RASTERIZATION FOR PARENTS
@python_app
def create_composite_geotiffs(tiles, config, logging_dict = logging_dict):
    """
    Make composite geotiffs (step 3)
    """
    import pdgraster
    if logging_dict:
        import logging.config
        logging.config.dictConfig(logging_dict)
    rasterizer = pdgraster.RasterTiler(config)
    return rasterizer.parent_geotiffs_from_children(tiles, recursive=False)

# Can't start lower z-level until higher z-level is complete.
for z in parent_zs:

    # Determine which tiles we need to make for the next z-level based on the
    # path names of the files just created
    child_paths = tile_manager.get_filenames_from_dir('geotiff', z=z + 1)
    parent_tiles = set()
    for child_path in child_paths:
        parent_tile = tile_manager.get_parent_tile(child_path)
        parent_tiles.add(parent_tile)
    parent_tiles = list(parent_tiles)

    # Break all parent tiles at level z into batches
    parent_tile_batches = make_batch(parent_tiles, batch_size_geotiffs)

    # Make the next level of parent tiles
    app_futures = []
    for parent_tile_batch in parent_tile_batches:
        app_future = create_composite_geotiffs(
            parent_tile_batch, workflow_config, logging_dict)
        app_futures.append(app_future)

    # Don't start the next z-level, and don't move to step 4, until the
    # current z-level is complete
    [a.result() for a in app_futures]

logger.info(f'rasterization for lower z-levels done!')

# update ranges before web tiling to ensure color ranges are standard across each z-level
rasterizer.update_ranges()

logger.info('updated ranges!')

updated_workflow_config = rasterizer.config.config

logger.info('updated workflow config! Moving on to web tiling')

# WEB TILING
geotiff_paths = tile_manager.get_filenames_from_dir('geotiff')
geotiff_batches = make_batch(geotiff_paths, batch_size_web_tiles)

@python_app
def create_web_tiles(geotiff_paths, config, logging_dict=logging_dict):
    """
    Create a batch of webtiles from geotiffs (step 4)
    """
    import pdgraster
    if logging_dict:
        import logging.config
        logging.config.dictConfig(logging_dict)
    rasterizer = pdgraster.RasterTiler(config)
    return rasterizer.webtiles_from_geotiffs(
        geotiff_paths, update_ranges=False)

app_futures = []
for batch in geotiff_batches:
    app_future = create_web_tiles(batch, updated_workflow_config, logging_dict)
    app_futures.append(app_future)

[a.result() for a in app_futures]

logger.info('web tiling done!')

htex_config_local.executors[0].shutdown()
parsl.clear()

logger.info('shutdown parsl. End of script.')