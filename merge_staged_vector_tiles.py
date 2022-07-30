"""
This file prepares staged, vector tiles for archiving in the DataONE network.
It has methods that do the following:
    1. Find all the paths of vector tiles in the staged directory.
    2. Open each path in GeoPandas
    3. Remove the polygons whose centroids are not contained within the
       boundaries of the tile. (This avoids archiving the exact same polygons
       in two different tiles, in the case that the polygon intersects a tile's
       boundary.)
    4. Remove the "centroids_within_tile" and "tile" columns, which are no
       longer needed after the above step. The tile is still identified using
       by the "centroid_tile" property.
    5. Write the GeoPandas object to a file in a given archive directory, still
       maintaining the tile path structure (e.g.
       {TileMatrix}/{TileRow}/{TileCol})
"""

import os
import shutil
import time.monotonic
from ast import Raise

import geopandas as gpd
import pandas as pd
import pdgstaging.TilePathManager
import pyfastcopy  # monky patch shutil for faster copy
import ray
from pdgstaging.TileStager import TileStager

#######################
#### Change me 😁  ####
#######################
RAY_ADDRESS       = 'ray://172.28.23.102:10001'  # SET ME!! Ray head-node IP address (using port 10001). Use output from `$ ray start --head --port=6379 --dashboard-port=8265`
NUM_PARALLEL_CPU_CORES = 3000 # 220 # 220/960 = 23% 220

# ALWAYS include the tailing slash "/"
BASE_DIR_OF_INPUT = '/ime/bbki/kastanday/maple_data_xsede_bridges2/outputs/'   # The output data of MAPLE. Which is the input data for STAGING.
FOOTPRINTS_PATH   = BASE_DIR_OF_INPUT + 'footprints/staged_footprints/'

OUTPUT            = '/tmp/viz_output/july_24_v2/'       # Dir for results. High I/O is good.
# OUTPUT            = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_13_FULL_SINGLE_RUN/gpub052/july_13_fifteennode/'       # Dir for results.
# OUTPUT            = '/ime/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_24_v2/'       # Dir for results.
OUTPUT_OF_STAGING = OUTPUT + 'staged/'              # Output dirs for each sub-step
GEOTIFF_PATH      = OUTPUT + 'geotiff/'
WEBTILE_PATH      = OUTPUT + 'web_tiles/'
THREE_D_PATH      = OUTPUT + '3d_tiles/'

# Convenience for little test runs. Change me 😁  
# ONLY_SMALL_TEST_RUN = False                          # For testing, this ensures only a small handful of files are processed.
# TEST_RUN_SIZE       = 10                              # Number of files to pre processed during testing (only effects testing)
##############################
#### END OF Change me 😁  ####
##############################

##################################
#### ⛔️ don't change these ⛔️  ####
##################################
IWP_CONFIG = {"dir_input": BASE_DIR_OF_INPUT,"ext_input": ".shp","dir_footprints": FOOTPRINTS_PATH,"dir_geotiff": GEOTIFF_PATH,"dir_web_tiles": WEBTILE_PATH,"dir_staged": OUTPUT_OF_STAGING,"filename_staging_summary": OUTPUT_OF_STAGING + "staging_summary.csv","filename_rasterization_events": GEOTIFF_PATH + "raster_events.csv","filename_rasters_summary": GEOTIFF_PATH + "raster_summary.csv","version": datetime.now().strftime("%B%d,%Y"),"simplify_tolerance": 0.1,"tms_id": "WorldCRS84Quad","z_range": [0, 16],"geometricError": 57,"z_coord": 0,"statistics": [    {        "name": "iwp_count",        "weight_by": "count",        "property": "centroids_per_pixel",        "aggregation_method": "sum",        "resampling_method": "sum",        "val_range": [0, None],        "palette": ["#66339952", "#d93fce", "#ffcc00"],        "nodata_val": 0,        "nodata_color": "#ffffff00"    },    {        "name": "iwp_coverage",        "weight_by": "area",        "property": "area_per_pixel_area",        "aggregation_method": "sum",        "resampling_method": "average",        "val_range": [0, 1],        "palette": ["#66339952", "#ffcc00"],        "nodata_val": 0,        "nodata_color": "#ffffff00"    },],"deduplicate_at": ["raster", "3dtiles"],"deduplicate_keep_rules": [["Date", "larger"]],"deduplicate_method": "footprints",}


def main():
    # import argparse
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--path', type=str, required=True)
    # parser.add_argument('--stager', type=str, required=True)
    # parser.add_argument('--archive_dir', type=str, required=True)
    # parser.add_argument('--ext', type=str, required=False)
    # args = parser.parse_args()
    # archive_vector_tile(args.path, args.stager, args.archive_dir, args.ext)
    # merge_all_staged_dirs(args.path, args.stager, args.archive_dir, args.ext)
    
    # todo: 
    # 1. add more paths to staged_dir_paths_list
    
    staged_dir_paths_list = ['/ime/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_13_FULL_SINGLE_RUN/gpub050/july_13_fifteennode/staged',
                            ]
    merged_dir_path = '/ime/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_13_FULL_SINGLE_RUN/merged/july_13_fifteennode/staged'
    stager = pdgstaging.TileStager(config=IWP_CONFIG, check_footprints=False)
    ext = '.gpkg'
    
    merge_all_staged_dirs(staged_dir_paths_list, merged_dir_path, stager, ext)


def collect_paths_from_dir(path_manager, base_dir_name_string, base_dir_path, ext):
    '''
    Collect all the paths from a directory that match an extension. 
    
    Parameters
    ----------
    path_manager 
        `path_manager = TileStager(stager).tiles`
    base_dir_name_string
        A human-readable name to use as an alias for this path ()
    merged_dir_path
    
    '''
    
    start = time.monotonic()
    print(f"Collecting paths in basedir named {base_dir_name_string}, recursively from base path: {base_dir_path}")
    path_manager.add_base_dir(base_dir_name_string, base_dir_path, ext)
    paths_list = path_manager.get_filenames_from_dir(base_dir_name_string)
    paths_set = set(paths_list) # speed optimization
    
    assert len(paths_list) == len(paths_set), f"❌ Warning: There are duplicate paths in this base dir {base_dir_path}"
    
    print("Done collecting paths for one iteration.")
    print(f"⏰ Elapsed time: {(time.monotonic() - start)/60:.2f}")
    
    return paths_list, paths_set

def merge_all_staged_dirs(staged_dir_paths_list, merged_dir_path, stager, ext=None):
    """
    Merge a set of 'staged' dirs from different compute nodes into the final output.
    This is mainly appending to staged vector geopandas files. 

    Parameters
    ----------
    staged_dir_paths_list : List[str]
        A list of paths to the directories containing the staged vector tiles. E.g. path to 'staged'
    merged_dir_path : str
        The path that will contain a merged version of all 'staged' dirs. 
        Recommended to start with one of the 'staged' dirs. 
    stager : TileStager object or dict
        The TileStager object used to stage the vector tiles, or a dictionary
        of the TileStager configuration.
    ext : str, optional
        The extension to use for the archive file. If not provided, the
        extension will be determined from the path.

    Returns
    -------
    archive_paths : list
        A list of paths to the archived files.

    """
    merge_all_vector_tiles_start_time = time.monotonic()
    
    # use same stager for whole merge job
    if isinstance(stager, (dict, str)):
        stager = TileStager(stager)
    path_manager = stager.tiles
    
    # collect paths
    final_merged_paths, final_merged_paths_set = collect_paths_from_dir(path_manager, 'merged_dir_path', merged_dir_path, ext)
    
    # MERGE! For each staged_dir! 
    for i, staged_dir_path in enumerate(staged_dir_paths_list):
        
        # collect paths
        paths, paths_set = collect_paths_from_dir(path_manager, f'staged_dir_path{i}', staged_dir_path, ext)
        
        for incoming_tile_in_path in paths:
            # If file not exist, copy/create new.
            # if path already there, check if identical.
                # if not identical, merge/append.
                # if identical, skip.
            
            incoming_tile_out_path = path_manager.path_from_tile(tile=incoming_tile_in_path, base_dir='merged_dir_path')
            
            # todo check that this comparison is lining up...
            if incoming_tile_out_path not in final_merged_paths_set:
                # (1) add to final_merged_paths_set
                # (2) copy incoming to destination 
                final_merged_paths_set.add(incoming_tile_out_path)
                # faster with pyfastcopy
                shutil.copyfile(incoming_tile_in_path, incoming_tile_out_path)
            else:
                # merge
                print("""Merging
                      incoming_tile_in_path:  {incoming_tile_in_path}
                      incoming_tile_out_path: {incoming_tile_out_path}""")
                incoming_tile = path_manager.tile_from_path(incoming_tile_in_path) 
                final_tile    = path_manager.tile_from_path(incoming_tile_out_path)
                                
                if incoming_tile == final_tile:
                    # skip 
                    print("⚠️⚠️ Skipping... In & out are identical (paths above). ⚠️")
                    pass
                else:
                    # not same tile... append new polygons to existing tile...
                    print("Appending...")
                    start_append = time.monotonic()
                    incoming_tile.to_file(incoming_tile_out_path, mode='a')
                    print(f" ---> Append takes: {(time.monotonic() - start_append)} seconds")

    print(f"👉 Overall runtime of merge_all_vector_tiles() ⏰ Time: {(time.monotonic() - merge_all_vector_tiles_start_time)/60:.2f} minutes.")
    print("Done")
    return # archive_vector_tiles(paths, stager, archive_dir, ext)

if __name__=='__main__':
    main()


# def filter_gdf(gdf, filter_dict):
#     """
#     Filter a GeoPandas object by a dictionary of column names and values.

#     Parameters
#     ----------
#     gdf : GeoPandas object filter_dict : dict
#         A dictionary of column names and values to filter by.

#     Returns
#     -------
#     gdf : GeoPandas object
#     """
#     return gdf.loc[(gdf[list(filter_dict)] ==
#                    pd.Series(filter_dict)).all(axis=1)]


# def remove_columns(gdf, columns):
#     """
#     Remove columns from a GeoPandas object.

#     Parameters
#     ----------
#     gdf : GeoPandas object columns : list of str
#         A list of column names to remove.

#     Returns
#     -------
#     gdf : GeoPandas object
#     """
#     return gdf.drop(columns, axis=1)


# def prepare_for_archiving(path, stager):
#     """
#     Prepare a vector tile for archiving. This involves:
#         1. Removing the polygons whose centroids are not contained within the
#            boundaries of the tile.
#         2. Removing the "centroids_within_tile" and "tile" columns

#     Parameters
#     ----------
#     path : str
#         The path to the vector tile to prepare for archiving.
#     stager : TileStager object or dict
#         The TileStager object used to stage the vector tiles, or a dictionary
#         of the TileStager configuration.

#     Returns
#     -------
#     gdf : GeoPandas object
#     """
#     gdf = gpd.read_file(path)
#     if isinstance(stager, (dict, str)):
#         stager = TileStager(stager)
#     config = stager.config
#     prop_centroid_within_tile = config.polygon_prop('centroid_within_tile')
#     prop_tile = stager.config.polygon_prop('tile')
#     gdf = filter_gdf(gdf, {prop_centroid_within_tile: True})
#     gdf = remove_columns(gdf, [prop_centroid_within_tile, prop_tile])
#     return gdf


# def archive_vector_tile(path, stager, archive_dir, ext=None):
#     """
#     Archive a single vector tile.

#     Parameters
#     ----------
#     path : str
#         The path to the vector tile to archive.
#     stager : TileStager object or dict
#         The TileStager object used to stage the vector tiles, or a dictionary
#         of the TileStager configuration.
#     archive_dir : str
#         The path to the directory to archive the vector tile to.
#     ext : str, optional
#         The extension to use for the archive file. If not provided, the
#         extension will be determined from the path.

#     Returns
#     -------
#     archive_path : str
#         The path to the archived file.

#     """
#     if isinstance(stager, (dict, str)):
#         stager = TileStager(stager)
#     if ext is None:
#         ext = os.path.splitext(path)[1]
#     path_manager = stager.tiles
#     if 'archive' not in path_manager.base_dirs:
#         path_manager.add_base_dir('archive', archive_dir, ext)
#     out_path = path_manager.path_from_tile(tile=path, base_dir='archive')
#     gdf = prepare_for_archiving(path, stager)
#     # mk di
#     stager.tiles.create_dirs(out_path)
#     gdf.to_file(out_path)
#     return out_path


# def archive_vector_tiles(paths, stager, archive_dir, ext=None):
#     """
#     Archive a list of vector tiles.

#     Parameters
#     ----------
#     path : str
#         The path to the vector tile to archive.
#     stager : TileStager object or dict
#         The TileStager object used to stage the vector tiles, or a dictionary
#         of the TileStager configuration.
#     archive_dir : str
#         The path to the directory to archive the vector tile to.
#     ext : str, optional
#         The extension to use for the archive file. If not provided, the
#         extension will be determined from the path.

#     Returns
#     -------
#     archive_path : list
#         A list of paths to the archived files.

#     """
#     out_paths = []
#     for path in paths:
#         out_path = archive_vector_tile(path, stager, archive_dir, ext)
#         out_paths.append(out_path)
#     return out_paths

# import pathlib


# # @python_app
# def archive_vector_tile_parsl(path, stager, archive_dir, ext=None):
#     """
#     Archive a single vector tile as a Parsl task.

#     Parameters
#     ----------
#     path : str
#         The path to the vector tile to archive.
#     stager : dict
#         A dictionary of the TileStager configuration.
#     archive_dir : str
#         The path to the directory to archive the vector tile to.
#     ext : str, optional
#         The extension to use for the archive file. If not provided, the
#         extension will be determined from the path.

#     Returns
#     -------
#     archive_path : parsl.app.futures.DataFuture
#         A future that will contain the path to the archived file as the result.

#     """
#     if isinstance(stager, (dict, str)):
#         stager = TileStager(stager)
#     if ext is None:
#         ext = os.path.splitext(path)[1]
#     path_manager = stager.tiles
#     if 'archive' not in path_manager.base_dirs:
#         path_manager.add_base_dir('archive', archive_dir, ext)
#     out_path = path_manager.path_from_tile(tile=path, base_dir='archive')
#     gdf = prepare_for_archiving(path, stager)
#     stager.tiles.create_dirs(out_path)
#     gdf.to_file(out_path)

#     # prepare for archiving
#     gdf = gpd.read_file(path)
#     config = stager.config
#     prop_centroid_within_tile = config.polygon_prop('centroid_within_tile')
#     prop_tile = stager.config.polygon_prop('tile')
#     gdf = filter_gdf(gdf, {prop_centroid_within_tile: True})
#     gdf = remove_columns(gdf, [prop_centroid_within_tile, prop_tile])

#     gdf.to_file(out_path)
#     return out_path


# def archive_vector_tiles_parsl(paths, stager, archive_dir, ext=None):
#     """
#     Archive a list of vector tiles in parallel with Parsl.

#     Parameters
#     ----------
#     path : str
#         The path to the vector tile to archive.
#     stager : dict
#         A dictionary of the TileStager configuration.
#     archive_dir : str
#         The path to the directory to archive the vector tile to.
#     ext : str, optional
#         The extension to use for the archive file. If not provided, the
#         extension will be determined from the path.

#     Returns
#     -------
#     archive_paths : list of parsl.app.futures.DataFuture
#         A list of futures that will contain the paths to the archived files as
#         the result of each future.

#     """
#     data_futures = []
#     for path in paths:
#         data_future = archive_vector_tile_parsl(path, stager, archive_dir, ext)
#         data_futures.append(data_future)
#     return data_futures


# def archive_all_vector_tiles_parsl(stager, archive_dir, ext=None):
#     """
#     Archive all vector tiles in the staged directory in parallel with Parsl.

#     Parameters
#     ----------
#     path : str
#         The path to the vector tile to archive.
#     stager : dict
#         A dictionary of the TileStager configuration.
#     archive_dir : str
#         The path to the directory to archive the vector tile to.
#     ext : str, optional
#         The extension to use for the archive file. If not provided, the
#         extension will be determined from the path.

#     Returns
#     -------
#     archive_paths : list of parsl.app.futures.DataFuture
#         A list of futures that will contain the paths to the archived files as
#         the result of each future.

#     """
#     if isinstance(stager, (dict, str)):
#         stager = TileStager(stager)
#     path_manager = stager.tiles
#     paths = path_manager.get_filenames_from_dir('staged')
#     config = stager.config.input_config
#     return archive_vector_tiles_parsl(paths, config, archive_dir, ext)
