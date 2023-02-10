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

import filecmp
import os
import pathlib
import shutil
import time
import warnings
from ast import Raise
from datetime import datetime
import logging
import logging.config

import geopandas as gpd
import pandas as pd
import pdgstaging.TilePathManager
import pyfastcopy  # monky patch shutil for faster copy
import ray
from filelock import FileLock, Timeout
from pdgstaging.TileStager import TileStager
import subprocess
import pprint

###################################
#### ⛔️ don't change these ⛔️  ####
###################################
# setup ray
result = subprocess.run(["hostname", "-i"], capture_output=True, text=True)
head_ip = result.stdout.strip()
print(f"Connecting to Ray... at address ray://{head_ip}:10001")
ray.init(address=f'ray://{head_ip}:10001', dashboard_port=8265)   # most reliable way to start Ray
# use port-forwarding to see dashboard: `ssh -L 8265:localhost:8265 kastanday@kingfisher.ncsa.illinois.edu`
assert ray.is_initialized() == True
print("🎯 Ray initialized.")

# todo: see if I can delete all of these constants....
# These don't matter much in this workflow.
# ALWAYS include the tailing slash "/"
# BASE_DIR_OF_INPUT = '/tmp/'  
# FOOTPRINTS_PATH   = BASE_DIR_OF_INPUT + 'staged_footprints/'

# OUTPUT            = '/tmp/' # Dir for results. High I/O is good.
# Output dirs for each sub-step:
# OUTPUT_OF_STAGING = OUTPUT + 'staged/' 
# GEOTIFF_PATH      = OUTPUT + 'geotiff/'
# WEBTILE_PATH      = OUTPUT + 'web_tiles/'
# THREE_D_PATH      = OUTPUT + '3d_tiles/'
# IWP_CONFIG = {"dir_input": BASE_DIR_OF_INPUT,"ext_input": ".shp", "dir_footprints": FOOTPRINTS_PATH,"dir_geotiff": GEOTIFF_PATH,"dir_web_tiles": WEBTILE_PATH,"dir_staged": OUTPUT_OF_STAGING,"filename_staging_summary": OUTPUT_OF_STAGING + "staging_summary.csv","filename_rasterization_events": GEOTIFF_PATH + "raster_events.csv","filename_rasters_summary": GEOTIFF_PATH + "raster_summary.csv","version": datetime.now().strftime("%B%d,%Y"),"simplify_tolerance": 0.1,"tms_id": "WorldCRS84Quad","z_range": [0, 16],"geometricError": 57,"z_coord": 0,"statistics": [    {        "name": "iwp_count",        "weight_by": "count",        "property": "centroids_per_pixel",        "aggregation_method": "sum",        "resampling_method": "sum",        "val_range": [0, None],        "palette": ["#66339952", "#d93fce", "#ffcc00"],        "nodata_val": 0,        "nodata_color": "#ffffff00"    },    {        "name": "iwp_coverage",        "weight_by": "area",        "property": "area_per_pixel_area",        "aggregation_method": "sum",        "resampling_method": "average",        "val_range": [0, 1],        "palette": ["#66339952", "#ffcc00"],        "nodata_val": 0,        "nodata_color": "#ffffff00"    },],"deduplicate_at": ["raster", "3dtiles"],"deduplicate_keep_rules": [["Date", "larger"]],"deduplicate_method": "footprints",}

# Juliet's edit: commented out Kastan's manual config settings to instead import config for workflow
import PRODUCTION_IWP_CONFIG
IWP_CONFIG = PRODUCTION_IWP_CONFIG.IWP_CONFIG
# update the config for the current context
IWP_CONFIG['dir_staged'] = IWP_CONFIG['dir_staged_remote']
IWP_CONFIG['dir_footprints'] = IWP_CONFIG['dir_footprints_local']

def main():
    '''
    Usage Instructions:
        - Set the list of paths to the vector tiles `staged_dir_paths_list`
        - Choose one of the paths from the last step, remove it from the `staged_dir_paths_list` and set it as the `merged_dir_path` that will be used as the merged dir.
    '''
    
    #######################
    #### Change me 😁  ####
    #######################
    # todo -- get files from dirs automatically, using os.lsdir().
    #BASE_DIR = '/scratch/bbou/julietcohen/IWP/output/...'
    merged_dir_path = f"{IWP_CONFIG['dir_staged']}cn003"  # this path SHOULD NOT be in the `staged_dir_paths_list`
    staged_dir_paths_list = [
        f"{IWP_CONFIG['dir_staged']}cn005",
        f"{IWP_CONFIG['dir_staged']}cn006",
     #   f"{IWP_CONFIG['dir_staged']}gpub092",
     #   f"{IWP_CONFIG['dir_staged']}gpub093",
     #   f"{IWP_CONFIG['dir_staged']}gpub094",
     #   f"{IWP_CONFIG['dir_staged']}gpub095",
     #   f"{IWP_CONFIG['dir_staged']}gpub096",
     #   f"{IWP_CONFIG['dir_staged']}gpub097",
     #   f"{IWP_CONFIG['dir_staged']}gpub098",
    ]
    ##############################
    #### END OF Change me 😁  ####
    ##############################
    
    # validate input
    for path in staged_dir_paths_list + [merged_dir_path]:
        # check that path exists
        if not os.path.exists(path):
            raise ValueError(f'Path {path} does not exist. For usage instructions, read the top of this file.')
    
    print("Input dirs: ", "\n".join(staged_dir_paths_list))
    print("Final  dir: ", merged_dir_path)
    
    #stager = pdgstaging.TileStager(config=IWP_CONFIG, check_footprints=False)
    stager = TileStager(config=IWP_CONFIG, check_footprints=False) # changed this from above line when troubleshooting merging 02/01
    ext = '.gpkg'
    
    print("Starting merge...")
    start_distributed_logging()
    merger = StagingMerger()
    merger.merge_all_staged_dirs(staged_dir_paths_list, merged_dir_path, stager, ext)

class StagingMerger():
    def __init__(self, ):
        self.final_merged_paths_set = None
        self.append_count = 0
        self.skipped_merge_identical_file_count = 0
        self.fast_copy_count = 0
        self.isDestructive = False  # Decide between mv and cp! 
        

    def merge_all_staged_dirs(self, staged_dir_paths_list, merged_dir_path, stager, ext=None):
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
            stager = TileStager(stager, check_footprints=False)
        path_manager = stager.tiles
        
        # collect paths
        final_merged_paths, final_merged_paths_set = self.collect_paths_from_dir(path_manager, 'merged_dir_path', merged_dir_path, ext)
        
        # MERGE! For each staged_dir (equal to number of original compute nodes)! 
        for i, staged_dir_path in enumerate(staged_dir_paths_list):
            
            # collect paths
            paths, paths_set = self.collect_paths_from_dir(path_manager, f'staged_dir_path{i}', staged_dir_path, ext)
            incoming_length = len(paths_set)
            
            print(f"Merged dir paths: {len(final_merged_paths)}")
            print(f"Incoming staged dir paths ({i + 1} of {len(staged_dir_paths_list)}): {incoming_length}")

            try: 
                # Start remote functions
                app_futures = []
                
                path_batches = make_batch(paths, batch_size=800)
                total_batches = len(path_batches)
                for j, incoming_tile_in_path_batch in enumerate(path_batches):
                    print(f'starting batch {j+1} of {total_batches}')  
                    
                    # collect all the out paths
                    incoming_tile_out_path_batch = []
                    for incoming_tile_in_path in incoming_tile_in_path_batch:
                        incoming_tile_out_path_batch.append( path_manager.path_from_tile(tile=incoming_tile_in_path, base_dir='merged_dir_path') )
                    
                    #app_future = batch_merge_tile.remote(incoming_tile_in_path_batch, incoming_tile_out_path_batch, isDestructive=self.isDestructive, stager.config.input_config)
                    # batch_merge_tile(incoming_tile_in_path_batch, incoming_tile_out_path_batch, isDestructive, stager):                    
                    app_future = batch_merge_tile.remote(incoming_tile_in_path_batch, incoming_tile_out_path_batch, isDestructive=self.isDestructive, stager=stager.config.input_config)
                    app_futures.append(app_future)
                # for incoming_tile_in_path in paths:
                #     NOT BATCHED VERSION:
                #     incoming_tile_out_path = path_manager.path_from_tile(tile=incoming_tile_in_path, base_dir='merged_dir_path')
                #     app_future = self.merge_tile.remote(incoming_tile_in_path, incoming_tile_out_path, self.isDestructive)
                #     app_futures.append(app_future)

                # collect results
                for k in range(0, len(app_futures)): 
                    ready, not_ready = ray.wait(app_futures)
                    
                    print(f"{ray.get(ready)}\n✅ Completed batch {k+1} of {total_batches}. Actions taken ^^")
                    print(f"using source dir     : {staged_dir_path}")
                    print(f"using destination dir: {merged_dir_path}")
                    # print(f"📌 Completed {i+1} of {incoming_length}")
                    # print(f"⏰ Running total of elapsed time: {(time.monotonic() - merge_all_vector_tiles_start_time)/60:.2f} minutes\n")

                    app_futures = not_ready
                    if not app_futures:
                        break
            except Exception as e:
                print(f"‼️‼️‼️‼️‼️ VERY BAD: Cauth error in Ray loop: {str(e)}")
                print(f"during processing of {staged_dir_path}")
                exit()
            finally:
                print(f"Runtime: {(time.monotonic() - merge_all_vector_tiles_start_time):.2f} seconds")

            print(f'⏰ Total time to merge {incoming_length} tiles: {(time.monotonic() - merge_all_vector_tiles_start_time)/60:.2f} minutes\n')
                
        print("Done, exiting...")
        return 

    def collect_paths_from_dir(self, path_manager, base_dir_name_string, base_dir_path, ext):
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
        # This is where a cache of path lists (one for each compute node), will be stored.
        paths_list_local_filename = f'./path_list_cache/{base_dir_name_string}.txt'
        
        ## IF PATHS WERE SAVED to a file, much faster. ELSE: Collect all paths from NFS file server.
        paths_list = []
        pathlib_paths_list_local_filename = pathlib.Path(paths_list_local_filename)
        
        # DON'T USE A CACHE. CAUSES HARD BUGS. NO CACHE: Collect all paths from NFS file server.
        if False: # pathlib_paths_list_local_filename.exists():
            with open(paths_list_local_filename, 'r') as fp:
                for line in fp:
                    # remove linebreak from each line
                    paths_list.append(line[:-1])
            paths_set = set(paths_list) # speed optimization
            self.final_merged_paths_set = paths_set
            assert len(paths_list) == len(paths_set), f"❌ Warning: There are duplicate paths in this base dir: {base_dir_path}"
            path_manager.add_base_dir(base_dir_name_string, base_dir_path, ext)
        else:
            start = time.monotonic()
            print(f"Collecting paths. Base dir named: {base_dir_name_string}  \n\tpath: {base_dir_path}") # script got this far before not finding paths to merge, test runs 01/31 and 02/01
            path_manager.add_base_dir(base_dir_name_string, base_dir_path, ext) 
            paths_list = path_manager.get_filenames_from_dir(base_dir_name_string)
            paths_set = set(paths_list) # speed optimization
            self.final_merged_paths_set = paths_set
            
            assert len(paths_list) == len(paths_set), f"❌ Warning: There are duplicate paths in this base dir: {base_dir_path}"
            
            print(f"⏰ Elapsed time: {(time.monotonic() - start)/60:.2f} minutes.") # test run 01/31 script ptrinted this so issue is in lines 255-259
            
            # write path list to disk
            pathlib_paths_list_local_filename.parent.mkdir(parents=True, exist_ok=True)
            pathlib_paths_list_local_filename.touch(exist_ok=True)
            with open(paths_list_local_filename, "w") as outfile:
                # todo: write oritinal filepath too as first line!
                outfile.write("\n".join(str(path) for path in paths_list))
        
        return paths_list, paths_set
'''
###############################################
End class methods
###############################################
'''

def make_batch(items, batch_size):
    """
    Simple helper.
    Create batches of a given size from a list of items.
    """
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

@ray.remote
def batch_merge_tile(incoming_tile_in_path_batch, incoming_tile_out_path_batch, isDestructive, stager):
    '''
    Merge a batch of tiles. More efficient by making fewer ray remote calls.
    '''
    assert len(incoming_tile_in_path_batch) == len(incoming_tile_out_path_batch), f"❌ Error: the 'in-paths' and 'out-paths' must match. You gave me: {len(incoming_tile_in_path_batch)} != {len(incoming_tile_out_path_batch)}"
    actions_taken = []
    for incoming_tile_in_path, incoming_tile_out_path in zip(incoming_tile_in_path_batch, incoming_tile_out_path_batch):
        action_taken_string = merge_tile(incoming_tile_in_path, incoming_tile_out_path, isDestructive, stager)
        actions_taken.append(action_taken_string)
    
    return actions_taken

# @ray.remote
def merge_tile(incoming_tile_in_path, incoming_tile_out_path, isDestructive, stager):
    if isinstance(stager, (dict, str)):
        stager = TileStager(stager, check_footprints=False)
    # todo check that this comparison is lining up...
    action_taken_string = ''
    if not os.path.exists(incoming_tile_out_path):
        # time.sleep(5)
        # (1) add to final_merged_paths_set
        # (2) copy incoming to destination 
        
        # NO NEED: final_merged_paths_set.add(incoming_tile_out_path)
        
        # check the destination folder structure exists
        filepath = pathlib.Path(incoming_tile_out_path)
        if not filepath.parent.is_dir():
            filepath.parent.mkdir(parents=True, exist_ok=True)
        
        if isDestructive:
            shutil.move(incoming_tile_in_path, incoming_tile_out_path)
            # print("File moved.")
            # print("File not in dest. Moving to dest: ", incoming_tile_out_path)
            action_taken_string += 'fast move'
        else: 
            # faster with pyfastcopy
            shutil.copyfile(incoming_tile_in_path, incoming_tile_out_path)
            # print("File not in dest. Copying to dest: ", incoming_tile_out_path)
            # print("File coppied.")
            action_taken_string += 'fast copy'
    else:
        # if identical, skip. Else, merge/append-to the GDF.
        if filecmp.cmp(incoming_tile_in_path, incoming_tile_out_path):
            # identical, skip merge
            if isDestructive:
                os.remove(incoming_tile_in_path)
                action_taken_string += 'identical. Deleted old.'
            # self.skipped_merge_identical_file_count += 1
            else:
                action_taken_string += 'identical. skipped'
            # print("⚠️ Skipping... In & out are identical. ⚠️")
            # print("In: ", incoming_tile_in_path)
            # print("Out: ", incoming_tile_out_path)
        else:
            # not same tile... append new polygons to existing tile...
            with warnings.catch_warnings():
                # suppress 'FutureWarning: pandas.Int64Index is deprecated and will be removed from pandas in a future version. Use pandas.Index with the appropriate dtype instead.'
                warnings.simplefilter("ignore")
                
                in_path_lock = lock_file(incoming_tile_in_path)
                out_path_lock = lock_file(incoming_tile_out_path)
                
                # actually "merge" two tiles (via append operation)
                try:
                    incoming_gdf = gpd.read_file(incoming_tile_in_path)
                    dedup_method = stager.config.get_deduplication_method()
                    if dedup_method is not None:
                        mode = 'w'
                        incoming_gdf = stager.combine_and_deduplicate(
                            incoming_gdf, incoming_tile_out_path)
                    else:
                        mode = 'a'
                    incoming_gdf.to_file(incoming_tile_out_path, mode=mode)
                except Exception as e:
                    # todo: implement logging w/ ray's distributed logger. 
                    print("❌ Error: ", e)
                    print("❌ Error: ", incoming_tile_in_path)
                    print("❌ Error: ", incoming_tile_out_path)
                    print("one of the above files was probably corrupted.")
                    logging.error(f'Error: {e} \n\tIncoming path: {incoming_tile_in_path} \n\tOutput path: {incoming_tile_out_path}')
                    # exit()
                
                release_file(in_path_lock)
                release_file(out_path_lock)
            
                # if isDestructive:
                #     # delete original file
                
                # always delete after two tiles are merged, otherwise it's impossible to keep things consistent.
                os.remove(incoming_tile_in_path)
                
            # print("appended & old deleted.")
            action_taken_string += f'Merged and old deleted.'
            # self.append_count += 1
    return action_taken_string

def start_distributed_logging():
    '''
    In output directory. 
    '''
    log_filename = make_workflow_id('merge_staged_tiles')
    filepath = pathlib.Path(IWP_CONFIG['dir_staged'] + log_filename)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.touch(exist_ok=True)
    logging.basicConfig(level=logging.INFO, filename= IWP_CONFIG['dir_staged'] + 'workflow_log.txt', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')
    
def make_workflow_id(name: str) -> str:
    import pytz
    from datetime import datetime

    # Timezones: US/{Pacific, Mountain, Central, Eastern}
    # All timezones `pytz.all_timezones`. Always use caution with timezones.
    curr_time = datetime.now(pytz.timezone('US/Central'))
    return f"{name}-{str(curr_time.strftime('%h_%d,%Y@%H:%M'))}"

def lock_file(path):
    """
        Lock a file for writing.

        Parameters
        ----------
        path : str
            The path to the file to lock

        Returns
        -------
        lock : FileLock
            The lock object
    """
    lock_path = path + '.lock'
    lock = FileLock(lock_path)
    lock.acquire()
    return lock

def release_file(lock):
    """
        Release a file lock. Remove the lock file.

        Parameters
        ----------
        lock : FileLock
            The lock to release
    """
    lock.release()
    if os.path.exists(lock.lock_file):
        os.remove(lock.lock_file)

if __name__=='__main__':
    main()
