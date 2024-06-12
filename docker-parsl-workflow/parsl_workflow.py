# test docker image and orchestrate containers
# with kubernetes by running a version of 
# the workflow with a kubernetes parsl config
# processing 2 small overlapping IWP files

# documentation for parsl config:
# https://parsl.readthedocs.io/en/stable/userguide/configuring.html#kubernetes-clusters


from datetime import datetime
import time

import pdgstaging
import pdgraster
import workflow_config

import json
import logging
import logging.handlers
from pdgstaging import logging_config
import os

import parsl
from parsl import python_app
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import KubernetesProvider
from parsl.addresses import address_by_route
from kubernetes import client, config # NOTE: might need to import this? not sure
# from . import parsl_config # NOTE: might need to import this file if running python command from Dcokerfile?
from parsl_config import config_parsl_cluster

import shutil

import subprocess
from subprocess import Popen
user = subprocess.check_output("whoami").strip().decode("ascii")


# call parsl config and initiate k8s cluster
parsl.set_stream_logger()
# use default settings defined in parsl_config.py:
htex_kube = config_parsl_cluster()
parsl.load(htex_kube)

workflow_config = workflow_config.workflow_config


# print("Removing old directories and files...")
# TODO: Decide filepath here, /app/ or . ?
# using just dir names and filenames here because set WORKDIR as:
# /home/jcohen/viz-workflow/docker-parsl_workflow/app
# dir = "app/"
# old_filepaths = ["staging_summary.csv",
#                  "raster_summary.csv",
#                  "raster_events.csv",
#                  "config__updated.json",
#                  "log.log"]
# for old_file in old_filepaths:
#   if os.path.exists(old_file):
#       os.remove(old_file)

# # remove dirs from past run
# old_dirs = ["staged",
#             "geotiff",
#             "web_tiles"]
# for old_dir in old_dirs:
#   if os.path.exists(old_dir) and os.path.isdir(old_dir):
#       shutil.rmtree(old_dir)


def run_pdg_workflow(
    workflow_config,
    batch_size = 300
):
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
    batch_size: int
        How many input files, staged files, geotiffs, or web tiles should be included in a single creation
        task? (each task is run in parallel) Default: 300
    """

    start_time = datetime.now()

    logging.info("Staging initiated.")

    stager = pdgstaging.TileStager(workflow_config)
    #tile_manager = rasterizer.tiles
    tile_manager = stager.tiles
    config_manager = stager.config

    input_paths = stager.tiles.get_filenames_from_dir('input')
    print(f"Input paths are: {input_paths}")
    input_batches = make_batch(input_paths, batch_size)

    # Stage all the input files (each batch in parallel)
    app_futures = []
    for i, batch in enumerate(input_batches):
        print(f"batch is {batch}")
        app_future = stage(batch, workflow_config)
        app_futures.append(app_future)
        logging.info(f'Started job for batch {i} of {len(input_batches)}')

    # Don't continue to next step until all files have been staged
    [a.result() for a in app_futures]

    logging.info("Staging complete.")
    print("Staging complete.")

    # ----------------------------------------------------------------

    # Create highest geotiffs 
    rasterizer = pdgraster.RasterTiler(workflow_config)

    # Process staged files in batches
    logging.info(f'Collecting staged file paths to process...')
    staged_paths = tile_manager.get_filenames_from_dir('staged')
    logging.info(f'Found {len(staged_paths)} staged files to process.')
    staged_batches = make_batch(staged_paths, batch_size)
    logging.info(f'Processing staged files in {len(staged_batches)} batches.')

    app_futures = []
    for i, batch in enumerate(staged_batches):
        app_future = create_highest_geotiffs(batch, workflow_config)
        app_futures.append(app_future)
        logging.info(f'Started job for batch {i} of {len(staged_batches)}')

    # Don't move on to next step until all geotiffs have been created
    [a.result() for a in app_futures]

    logging.info("Rasterization highest complete. Rasterizing lower z-levels.")
    print("Rasterization highest complete. Rasterizing lower z-levels.")

    # ----------------------------------------------------------------

    # Rasterize composite geotiffs
    min_z = config_manager.get_min_z()
    max_z = config_manager.get_max_z()
    parent_zs = range(max_z - 1, min_z - 1, -1)

    # Can't start lower z-level until higher z-level is complete.
    for z in parent_zs:

        # Determine which tiles we need to make for the next z-level based on the
        # path names of the geotiffs just created
        logging.info(f'Collecting highest geotiff paths to process...')
        child_paths = tile_manager.get_filenames_from_dir('geotiff', z = z + 1)
        logging.info(f'Found {len(child_paths)} highest geotiffs to process.')
        # create empty set for the following loop
        parent_tiles = set()
        for child_path in child_paths:
            parent_tile = tile_manager.get_parent_tile(child_path)
            parent_tiles.add(parent_tile)
        # convert the set into a list
        parent_tiles = list(parent_tiles)

        # Break all parent tiles at level z into batches
        parent_tile_batches = make_batch(parent_tiles, batch_size)
        logging.info(f'Processing highest geotiffs in {len(parent_tile_batches)} batches.')

        # Make the next level of parent tiles
        app_futures = []
        for parent_tile_batch in parent_tile_batches:
            app_future = create_composite_geotiffs(
                parent_tile_batch, workflow_config)
            app_futures.append(app_future)

        # Don't start the next z-level, and don't move to web tiling, until the
        # current z-level is complete
        [a.result() for a in app_futures]

    logging.info("Composite rasterization complete. Creating web tiles.")
    print("Composite rasterization complete. Creating web tiles.")

    # ----------------------------------------------------------------

    # Process web tiles in batches
    logging.info(f'Collecting file paths of geotiffs to process...')
    geotiff_paths = tile_manager.get_filenames_from_dir('geotiff')
    logging.info(f'Found {len(geotiff_paths)} geotiffs to process.')
    geotiff_batches = make_batch(geotiff_paths, batch_size)
    logging.info(f'Processing geotiffs in {len(geotiff_batches)} batches.')

    app_futures = []
    for i, batch in enumerate(geotiff_batches):
        app_future = create_web_tiles(batch, workflow_config)
        app_futures.append(app_future)
        logging.info(f'Started job for batch {i} of {len(geotiff_batches)}')

    # Don't record end time until all web tiles have been created
    [a.result() for a in app_futures]

    end_time = datetime.now()
    logging.info(f'⏰ Total time to create all z-level geotiffs and web tiles: '
                 f'{end_time - start_time}')

# ----------------------------------------------------------------

# Define the parsl functions used in the workflow:

@python_app
def stage(paths, config):
    """
    Stage a file
    """
    from datetime import datetime
    import json
    import logging
    import logging.handlers
    import os
    import pdgstaging
    from pdgstaging import logging_config

    stager = pdgstaging.TileStager(config, check_footprints = False)
    for path in paths:
        stager.stage(path)
    return True

# Create highest z-level geotiffs from staged files
@python_app
def create_highest_geotiffs(staged_paths, config):
    """
    Create a batch of geotiffs from staged files
    """
    from datetime import datetime
    import json
    import logging
    import logging.handlers
    import os
    import pdgraster
    from pdgraster import logging_config

    # rasterize the vectors, highest z-level only
    rasterizer = pdgraster.RasterTiler(config)
    return rasterizer.rasterize_vectors(
        staged_paths, make_parents = False)
    # no need to update ranges if manually set val_range in viz config

# ----------------------------------------------------------------

# Create composite geotiffs from highest z-level geotiffs 
@python_app
def create_composite_geotiffs(tiles, config):
    """
    Create a batch of composite geotiffs from highest geotiffs
    """
    from datetime import datetime
    import json
    import logging
    import logging.handlers
    import os
    import pdgraster
    from pdgraster import logging_config

    rasterizer = pdgraster.RasterTiler(config)
    return rasterizer.parent_geotiffs_from_children(
        tiles, recursive = False)

# ----------------------------------------------------------------

# Create a batch of webtiles from geotiffs
@python_app
def create_web_tiles(geotiff_paths, config):
    """
    Create a batch of webtiles from geotiffs
    """

    from datetime import datetime
    import json
    import logging
    import logging.handlers
    import os
    import pdgraster
    from pdgraster import logging_config

    rasterizer = pdgraster.RasterTiler(config)
    return rasterizer.webtiles_from_geotiffs(
        geotiff_paths, update_ranges = False)
    # no need to update ranges if manually set val_range in workflow config

# ----------------------------------------------------------------

def make_batch(items, batch_size):
    """
    Create batches of a given size from a list of items.
    """
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

# ----------------------------------------------------------------

# logging.info(f'Starting PDG workflow: staging, rasterization, and web tiling')
# if __name__ == "__main__":
#     run_pdg_workflow(workflow_config)

# # transfer visualization log from /tmp to user dir
# # TODO: Automate the following destination path to be the mounted volume in the config
# # maybe do this by importing config script that specifies the filepath as a variable at the top
# # TODO: Decide filepath here, /app/ or . ?
# cmd = ['mv', '/tmp/log.log', '/home/jcohen/viz-workflow/docker-parsl_workflow/app/']
# # initiate the process to run that command
# process = Popen(cmd)

# ----------------------------------------------------------------

def main():

    '''Main program.'''

    # make job last a while with useless computation
    size = 30
    stat_results = []
    for x in range(size):
        for y in range(size):
            current_time = datetime.now()
            print(f'Schedule job at {current_time} for {x} and {y}')
            stat_results.append(calc_product_long(x, y))
            
    stats = [r.result() for r in stat_results]
    print(f"Sum of stats: {sum(stats)}")


@python_app
def calc_product_long(x, y):
    '''Useless computation to simulate one that takes a long time'''
    from datetime import datetime
    import time
    current_time = datetime.now()
    prod = x*y
    time.sleep(15)
    return(prod)


if __name__ == "__main__":
    main()

# ------------------------------------------


# Shutdown and clear the parsl executor
# htex_kube.executors[0].scale_in(htex_kube.executors[0].connected_blocks())
htex_kube.executors[0].shutdown()
parsl.clear()

print("Script complete.")