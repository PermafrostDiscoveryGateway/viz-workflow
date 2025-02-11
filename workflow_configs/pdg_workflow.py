"""Main module."""

from datetime import datetime
import logging.config
import json
import argparse

import parsl
from parsl import python_app
from parsl.config import Config

# from parsl.channels import LocalChannel
from parsl.executors import HighThroughputExecutor

# from parsl.executors.threads import ThreadPoolExecutor
# from parsl.providers import LocalProvider
from parsl.providers import KubernetesProvider
from parsl.addresses import address_by_route
from kubernetes import client, config

import pdgraster
import pdgstaging
from StagedTo3DConverter import StagedTo3DConverter

from shapely.geometry import box
import geopandas as gpd


def init_parsl():
    parsl.set_stream_logger()
    # from parslexec import local_exec
    # from parslexec import htex_kube

    htex_kube = Config(
        executors=[
            HighThroughputExecutor(
                label="kube-htex",
                cores_per_worker=1,
                max_workers=2,
                worker_logdir_root="/",
                # Address for the pod worker to connect back
                # address=address_by_route(),
                address="192.168.0.103",
                # address_probe_timeout=3600,
                worker_debug=True,
                provider=KubernetesProvider(
                    namespace="test",
                    # Docker image url to use for pods
                    image="mbjones/python3-parsl:0.2",
                    # Command to be run upon pod start, such as:
                    # 'module load Anaconda; source activate parsl_env'.
                    # or 'pip install parsl'
                    # worker_init='echo "Worker started..."; lf=`find . -name \'manager.log\'` tail -n+1 -f ${lf}',
                    worker_init='echo "Worker started..."',
                    # The secret key to download the image
                    # secret="YOUR_KUBE_SECRET",
                    # Should follow the Kubernetes naming rules
                    pod_name="parsl-worker",
                    nodes_per_block=1,
                    init_blocks=2,
                    min_blocks=1,
                    # Maximum number of pods to scale up
                    max_blocks=4,
                ),
            ),
        ]
    )
    # parsl.load(local_exec)
    parsl.load(htex_kube)


def run_pdg_workflow(
    workflow_config,
    logging_dict=None,
    batch_size_staging=1,
    batch_size_rasterization=30,
    batch_size_3dtiles=20,
    batch_size_parent_3dtiles=500,
    batch_size_geotiffs=200,
    batch_size_web_tiles=200,
):
    """
    Run the main PDG workflow

    Parameters
    ----------
    workflow_config : dict
        Configuration for the PDG staging workflow
    logging_dict : dict
        Logging configuration (to pass to the Parsl apps)
    batch_size_staging : int
        How many input files should be included in a single staging task? (each
        task is run in parallel.) Default: 1
    batch_size_rasterization : int
        How many staged vector tile files should be included in a single
        rasterization task? (each task is run in parallel.) Default: 30
    batch_size_geotiffs : int
        How many parent tiles should be included in a single composite geotiff
        creating task? (each task is run in parallel.) Default: 200
    batch_size_web_tiles : int
        How many webtiles should be included in a single web tile creating
        task? (each task is run in parallel.) Default: 200
    """

    stager = pdgstaging.TileStager(workflow_config)
    tiles3dmaker = StagedTo3DConverter(workflow_config)
    rasterizer = pdgraster.RasterTiler(workflow_config)
    tile_manager = stager.tiles
    config_manager = stager.config
    min_z = config_manager.get_min_z()
    max_z = config_manager.get_max_z()
    parent_zs = range(max_z - 1, min_z - 1, -1)

    # ================================================================
    # Start the workflow

    overall_start = datetime.now()

    # ================================================================
    # STEP 1: Stage all inputs

    # Get all paths to input files and create batches of paths
    input_paths = stager.tiles.get_filenames_from_dir("input")
    input_batches = make_batch(input_paths, batch_size_staging)

    start_time = datetime.now()

    # Stage all the input files (each batch in parallel)
    app_futures = []
    for batch in input_batches:
        app_future = stage(batch, workflow_config, logging_dict)
        app_futures.append(app_future)

    # Don't continue to step 2 until all files have been staged
    [a.result() for a in app_futures]

    end_time = datetime.now()
    logging.info(
        f"Total time to stage {len(input_paths)} files: " f"{end_time - start_time}"
    )

    # =================================================================================
    # STEP 2: Deduplicate & rasterize all staged tiles (only highest z-level)

    # Get paths to all the newly staged tiles
    staged_paths = stager.tiles.get_filenames_from_dir("staged")
    staged_batches = make_batch(staged_paths, batch_size_rasterization)

    start_time = datetime.now()

    app_futures = []
    for batch in staged_batches:
        app_future = rasterize(batch, workflow_config, logging_dict)
        app_futures.append(app_future)

    # Don't continue to step 3 until all tiles have been rasterized
    [a.result() for a in app_futures]

    end_time = datetime.now()
    logging.info(
        f"⏰ Total time to rasterize {len(staged_paths)} tiles: "
        f"{end_time - start_time}"
    )

    # =================================================================================
    # STEP 3: Create parent geotiffs for all z-levels (except highest)

    start_time = datetime.now()

    # Can't start lower z-level until higher z-level is complete.
    for z in parent_zs:

        # Determine which tiles we need to make for the next z-level based on the
        # path names of the files just created
        child_paths = tile_manager.get_filenames_from_dir("geotiff", z=z + 1)
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
                parent_tile_batch, workflow_config, logging_dict
            )
            app_futures.append(app_future)

        # Don't start the next z-level, and don't move to step 4, until the
        # current z-level is complete
        [a.result() for a in app_futures]

    end_time = datetime.now()
    logging.info(
        f"⏰ Total time to create parent geotiffs: " f"{end_time - start_time}"
    )

    # =================================================================================
    # STEP 4: Create web tiles from geotiffs

    start_time = datetime.now()

    # Update ranges
    rasterizer.update_ranges()

    # Process web tiles in batches
    geotiff_paths = tile_manager.get_filenames_from_dir("geotiff")
    geotiff_batches = make_batch(geotiff_paths, batch_size_web_tiles)

    app_futures = []
    for batch in geotiff_batches:
        app_future = create_web_tiles(batch, workflow_config, logging_dict)
        app_futures.append(app_future)

    # Don't record end time until all web tiles have been created
    [a.result() for a in app_futures]

    end_time = datetime.now()

    logging.info(f"⏰ Total time to create web tiles: " f"{end_time - start_time}")

    # =================================================================================
    # STEP 5: Deduplicate & make leaf 3D tiles all staged tiles (only highest
    # z-level).
    # TODO: COMBINE WITH STEP 2, so we only read in and deduplicate
    # each staged file once.

    # Get paths to all the newly staged tiles
    staged_paths = stager.tiles.get_filenames_from_dir("staged")
    staged_batches = make_batch(staged_paths, batch_size_3dtiles)

    start_time = datetime.now()

    app_futures = []
    for batch in staged_batches:
        app_future = create_leaf_3dtiles(batch, workflow_config, logging_dict)
        app_futures.append(app_future)

    # Don't continue to step 6 until all max-zoom level 3d tilesets have been
    # created
    [a.result() for a in app_futures]

    end_time = datetime.now()
    logging.info(
        f"⏰ Total time to create {len(staged_paths)} 3d tiles: "
        f"{end_time - start_time}"
    )

    # =================================================================================
    # STEP 6: Create parent cesium 3d tilesets for all z-levels (except highest)
    # TODO: COMBINE WITH STEP 3

    start_time = datetime.now()

    # For tiles in max-z-level, get the total bounds for all the tiles. We will
    # limit parent tileset bounding volumes to this total bounds.
    # convert the paths to tiles
    max_z_tiles = [tile_manager.tile_from_path(path) for path in staged_paths]
    # get the total bounds for all the tiles
    max_z_bounds = [tile_manager.get_bounding_box(tile) for tile in max_z_tiles]
    # get the total bounds for all the tiles
    polygons = [
        box(bounds["left"], bounds["bottom"], bounds["right"], bounds["top"])
        for bounds in max_z_bounds
    ]
    max_z_bounds = gpd.GeoSeries(polygons, crs=tile_manager.tms.crs)

    bound_volume_limit = max_z_bounds.total_bounds

    # Can't start lower z-level until higher z-level is complete.
    for z in parent_zs:

        # Determine which tiles we need to make for the next z-level based on the
        # path names of the files just created
        all_child_paths = tiles3dmaker.tiles.get_filenames_from_dir("3dtiles", z=z + 1)
        parent_tiles = set()
        for child_path in all_child_paths:
            parent_tile = tile_manager.get_parent_tile(child_path)
            parent_tiles.add(parent_tile)
        parent_tiles = list(parent_tiles)

        # Break all parent tiles at level z into batches
        parent_tile_batches = make_batch(parent_tiles, batch_size_parent_3dtiles)

        # Make the next level of parent tiles
        app_futures = []
        for parent_tile_batch in parent_tile_batches:
            app_future = create_parent_3dtiles(
                parent_tile_batch, workflow_config, bound_volume_limit, logging_dict
            )
            app_futures.append(app_future)

        # Don't start the next z-level until the current z-level is complete
        [a.result() for a in app_futures]

    # Make a top-level tileset.json file - essential when the min_z level
    # comprises 2+ tiles, useful in any case so that the path to use in cesium
    # is consistently path/to/3dtiles/dir/tileset.json
    tiles3dmaker.make_top_level_tileset()

    end_time = datetime.now()
    logging.info(
        f"⏰ Total time to create parent 3d tiles: " f"{end_time - start_time}"
    )

    # ================================================================
    # End the workflow

    overall_end = datetime.now()
    total_time = overall_end - overall_start
    message = f"⏰ Total time to complete workflow: {total_time}"
    logging.info(message)
    print(message)


@python_app
def stage(paths, config, logging_dict=None):
    """
    Stage a file (step 1)
    """
    import pdgstaging

    if logging_dict:
        import logging.config

        logging.config.dictConfig(logging_dict)
    stager = pdgstaging.TileStager(config)
    for path in paths:
        stager.stage(path)
    return True


@python_app
def rasterize(staged_paths, config, logging_dict=None):
    """
    Rasterize a batch of vector files (step 2)
    """
    import pdgraster

    if logging_dict:
        import logging.config

        logging.config.dictConfig(logging_dict)
    rasterizer = pdgraster.RasterTiler(config)
    return rasterizer.rasterize_vectors(staged_paths, make_parents=False)


@python_app
def create_composite_geotiffs(tiles, config, logging_dict=None):
    """
    Make composite geotiffs (step 3)
    """
    import pdgraster

    if logging_dict:
        import logging.config

        logging.config.dictConfig(logging_dict)
    rasterizer = pdgraster.RasterTiler(config)
    return rasterizer.parent_geotiffs_from_children(tiles, recursive=False)


@python_app
def create_leaf_3dtiles(staged_paths, config, logging_dict=None):
    """
    Create a batch of leaf 3d tiles from staged vector tiles
    """
    from workflow_configs.pdg_workflow import StagedTo3DConverter

    if logging_dict:
        import logging.config

        logging.config.dictConfig(logging_dict)
    converter3d = StagedTo3DConverter(config)
    tilesets = []
    for path in staged_paths:
        ces_tile, ces_tileset = converter3d.staged_to_3dtile(path)
        tilesets.append(ces_tileset)
    return tilesets


@python_app
def create_parent_3dtiles(tiles, config, limit_bv_to=None, logging_dict=None):
    """
    Create a batch of cesium 3d tileset parent files that point to child
    tilesets
    """
    from workflow_configs.pdg_workflow import StagedTo3DConverter

    if logging_dict:
        import logging.config

        logging.config.dictConfig(logging_dict)
    converter3d = StagedTo3DConverter(config)
    return converter3d.parent_3dtiles_from_children(tiles, limit_bv_to)


# Create a batch of webtiles from geotiffs (step 4)
@python_app
def create_web_tiles(geotiff_paths, config, logging_dict=None):
    """
    Create a batch of webtiles from geotiffs (step 4)
    """
    import pdgraster

    if logging_dict:
        import logging.config

        logging.config.dictConfig(logging_dict)
    rasterizer = pdgraster.RasterTiler(config)
    return rasterizer.webtiles_from_geotiffs(geotiff_paths, update_ranges=False)


def make_batch(items, batch_size):
    """
    Create batches of a given size from a list of items.
    """
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def setup_logging(log_json_file):
    """
    Setup logging configuration
    """
    with open(log_json_file, "r") as f:
        logging_dict = json.load(f)
    logging.config.dictConfig(logging_dict)
    return logging_dict


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run the PDG visualization workflow.")
    parser.add_argument(
        "-c",
        "--config",
        help="Path to the pdg-viz configuration JSON file.",
        default="config.json",
        type=str,
    )
    parser.add_argument(
        "-l",
        "--logging",
        help="Path to the logging configuration JSON file.",
        default="logging.json",
        type=str,
    )
    args = parser.parse_args()

    logging_dict = setup_logging(args.logging)
    init_parsl()
    run_pdg_workflow(args.config, logging_dict)
