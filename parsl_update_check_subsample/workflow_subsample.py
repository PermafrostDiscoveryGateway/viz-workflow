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

# use sample of 30 random polygons from each of the 3 files from Ingmar
base_dir = Path('/home/jcohen/viz-workflow/parsl_update_check_subsample/data_subsample')
filename = 'lake_change_data.gpkg'
data_paths = [p.as_posix() for p in base_dir.glob('**/' + filename)]

workflow_config = '/home/jcohen/viz-workflow/parsl_update_check_subsample/ice-wedge-polygons.json'

# logging setup
logging_config = '/home/jcohen/viz-workflow/parsl_update_check_subsample/logging.json'

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

class StagedTo3DConverter():
    """
        Processes staged vector data into Cesium 3D tiles according to the
        settings in a config file or dict. This class acts as the orchestrator
        of the other viz-3dtiles classes, and coordinates the sending and
        receiving of information between them.
    """

    def __init__(
        self,
        config
    ):
        """
            Automatically initialize the StagedTo3DConverter class by appying the configuration when an object of that class is created.

            Parameters
            ----------
            self : need to explicitly state this parameter to pass any newly created object of class StagedTo3DConverter to the other paraneter (config)
                this is a python syntax requirement in order for the object to persist of this class

            config : dict or str
                A dictionary of configuration settings or a path to a config
                JSON file. (See help(pdgstaging.ConfigManager))

            Notes
            ----------
            - this function does not do the staging or tiling steps
        """

        self.config = pdgstaging.ConfigManager(config)
        self.tiles = pdgstaging.TilePathManager(
            **self.config.get_path_manager_config())

    def all_staged_to_3dtiles(
        self
    ):
        """
            Process all staged vector tiles into 3D tiles. This is simply a loop that iterates the function staged_to_rdtile() over all files in the staged directory.
        """

        # Get the list of staged vector tiles
        paths = self.tiles.get_filenames_from_dir('staged')
        # Process each tile
        for path in paths:
            self.staged_to_3dtile(path)

    def staged_to_3dtile(self, path):
        """
            Convert a staged vector tile into a B3DM tile file and a matching
            JSON tileset file.
            - the B3DM tile is applied to the PDG portal for visualization purposes
            - the JSON serves as the metadata for that tile

            Parameters
            ----------
            path : str
                The path to the staged vector tile.

            Returns
            -------
            tile, tileset : Cesium3DTile, Tileset
                The Cesium3DTiles and Cesium3DTileset objects
        """

        try:
            
            # Get information about the tile from the path
            tile = self.tiles.tile_from_path(path)
            out_path = self.tiles.path_from_tile(tile, '3dtiles')

            tile_bv = self.bounding_region_for_tile(tile) # bv = bounding volumne

            # Get the filename of the tile WITHOUT the extension
            tile_filename = os.path.splitext(os.path.basename(out_path))[0]
            # Get the base of the path, without the filename
            tile_dir = os.path.dirname(out_path) + os.path.sep

            # Log the event
            logger.info(
                f'Creating 3dtile from {path} for tile {tile} to {out_path}.')

            # Read in the staged vector tile
            gdf = gpd.read_file(path)

            # Summary of following steps:
            # Now that we have the path to the staged vector tile esptablished and logged, 
            # the following checks are executed on each staged vector tile:
            # 1. check if the tile has any data to start with
            # 2. check if the centroid of the polygons within the tile are within the tile boundaries, remove if not
            # 3. check if polygons within the tile overlap, deduplicate them if they do
            # 4. check if the tile has any data left if deduplication was executed
            # 5. if there were errors in the above steps, log that for debugging

            
            # Check if the gdf is empty
            if len(gdf) == 0:
                logger.warning(
                    f'Vector tile {path} is empty. 3D tile will not be'
                    ' created.')
                return

            # Remove polygons with centroids that are outside the tile boundary
            prop_cent_in_tile = self.config.polygon_prop(
                'centroid_within_tile')
            gdf = gdf[gdf[prop_cent_in_tile]]

            # Check if deduplication should be performed
            dedup_here = self.config.deduplicate_at('3dtiles')
            dedup_method = self.config.get_deduplication_method()

            # Deduplicate if required
            if dedup_here and (dedup_method is not None):
                dedup_config = self.config.get_deduplication_config(gdf)
                dedup = dedup_method(gdf, **dedup_config)
                gdf = dedup['keep']

                # The tile could theoretically be empty after deduplication
                if len(gdf) == 0:
                    logger.warning(
                        f'Vector tile {path} is empty after deduplication.'
                        ' 3D Tile will not be created.')
                    return

            # Create & save the b3dm file
            ces_tile, ces_tileset = TreeGenerator.leaf_tile_from_gdf(
                gdf,
                dir=tile_dir,
                filename=tile_filename,
                z=self.config.get('z_coord'),
                geometricError=self.config.get('geometricError'),
                tilesetVersion=self.config.get('version'),
                boundingVolume=tile_bv
            )

            return ces_tile, ces_tileset

        except Exception as e:
            logger.error(f'Error creating 3D Tile from {path}.')
            logger.error(e)

    def parent_3dtiles_from_children(self, tiles, bv_limit=None):
        """
            Create parent Cesium 3D Tileset json files that point to
            of child JSON files in the tile tree hierarchy.

            Parameters
            ----------
            tiles : list of morecantile.Tile
                The list of tiles to create parent tiles for.
        """

        tile_manager = self.tiles
        config_manager = self.config

        tileset_objs = []

        # Make the next level of parent tiles
        for parent_tile in tiles:
            # Get the path to the parent tile
            parent_path = tile_manager.path_from_tile(parent_tile, '3dtiles')
            # Get just the base dir without the filename
            parent_dir = os.path.dirname(parent_path)
            # Get the filename of the parent tile, without the extension
            parent_filename = os.path.basename(parent_path)
            parent_filename = os.path.splitext(parent_filename)[0]
            # Get the children paths for this parent tile
            child_paths = tile_manager.get_child_paths(parent_tile, '3dtiles')
            # Remove paths that do not exist
            child_paths = tile_manager.remove_nonexistent_paths(child_paths)
            # Get the parent bounding volume
            parent_bv = self.bounding_region_for_tile(
                parent_tile, limit_to=bv_limit)
            # If the bounding region is outside t
            # Get the version
            version = config_manager.get('version')
            # Get the geometric error
            geometric_error = config_manager.get('geometricError')
            # Create the parent tile
            tileset_obj = TreeGenerator.parent_tile_from_children_json(
                child_paths,
                dir=parent_dir,
                filename=parent_filename,
                geometricError=geometric_error,
                tilesetVersion=version,
                boundingVolume=parent_bv
            )
            tileset_objs.append(tileset_obj)

        return tileset_objs

    def bounding_region_for_tile(self, tile, limit_to=None):
        """
        For a morecantile.Tile object, return a BoundingVolumeRegion object
        that represents the bounding region of the tile.

        Parameters
        ----------
        tile : morecantile.Tile
            The tile object.
        limit_to : list of float
            Optional list of west, south, east, north coordinates to limit
            the bounding region to.

        Returns
        -------
        bv : BoundingVolumeRegion
            The bounding region object.
        """
        tms = self.tiles.tms
        bounds = tms.bounds(tile)
        bounds = gpd.GeoSeries(
            box(bounds.left, bounds.bottom, bounds.right, bounds.top),
            crs=tms.crs)
        if limit_to is not None:
            bounds_limitor = gpd.GeoSeries(
                box(limit_to[0], limit_to[1], limit_to[2], limit_to[3]),
                crs=tms.crs)
            bounds = bounds.intersection(bounds_limitor)
        bounds = bounds.to_crs(BoundingVolumeRegion.CESIUM_EPSG)
        bounds = bounds.total_bounds

        region_bv = {
            'west': bounds[0], 'south': bounds[1],
            'east': bounds[2], 'north': bounds[3],
        }
        return region_bv

# staging configuration
stager = pdgstaging.TileStager(workflow_config)
tile_manager = stager.tiles
config_manager = stager.config

# zoom levels configuration
min_z = config_manager.get_min_z()
max_z = config_manager.get_max_z()
parent_zs = range(max_z - 1, min_z - 1, -1)

# 3D tiler configuration
tiles3dmaker = StagedTo3DConverter(workflow_config)

# raster tilerconfiguration 
rasterizer = pdgraster.RasterTiler(workflow_config)

# set up parsl provider
# bash command to activate virtual environment
activate_env = 'source /home/jcohen/.bashrc; conda activate parsl_update_check'

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

# set batch sizes
batch_size_staging=1
batch_size_rasterization=30
batch_size_3dtiles=20
batch_size_parent_3dtiles=500
batch_size_geotiffs=200
batch_size_web_tiles=200

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

logger.info('staging done!')

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

# robyn's new addditon to workflow:
updated_workflow_config = rasterizer.config.config

logger.info('updated workflow config with Robyns new code addition, moving on to web tiling')

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