
import json
import logging
import logging.config
import os
import pathlib
import socket  # for seeing where jobs run when distributed
import sys
import time
import traceback
import warnings
from asyncio.log import logger
from collections import Counter
from contextlib import suppress
from copy import deepcopy
from datetime import datetime
import subprocess
import pprint

import pdgraster
import pdgstaging  # For staging
import ray
import viz_3dtiles  # import Cesium3DTile, Cesium3DTileset

# Convenience for little test runs. Change me 😁  
ONLY_SMALL_TEST_RUN = False                            # For testing, this ensures only a small handful of files are processed.
TEST_RUN_SIZE       = 10_000                              # Number of files to pre processed during testing (only effects testing)

# define user on Delta, avoid writing files to other user's dir
user = subprocess.check_output("whoami").strip().decode("ascii")

# help flag provides flag help
# store_true actions stores argument as True

##################################
#### ⛔️ don't change these ⛔️ ####
##################################
import PRODUCTION_IWP_CONFIG
IWP_CONFIG = PRODUCTION_IWP_CONFIG.IWP_CONFIG
print("Using config: ")
pprint.pprint(IWP_CONFIG)

def main():
    result = subprocess.run(["hostname", "-i"], capture_output=True, text=True)
    head_ip = result.stdout.strip()
    print(f"Connecting to Ray... at address ray://{head_ip}:10001")
    ray.init(address=f'ray://{head_ip}:10001', dashboard_port=8265)   # most reliable way to start Ray
    # use port-forwarding to see dashboard: `ssh -L 8265:localhost:8265 kastanday@kingfisher.ncsa.illinois.edu`
    assert ray.is_initialized() == True
    print("🎯 Ray initialized.")
    
    print(f"Write all results to output dir: {IWP_CONFIG['dir_output']}")
    print_cluster_stats()
    start_logging()
    start = time.time()
    
    try:
        ########## MAIN STEPS ##########
        print("Starting main...")
        
        # (optionally) Comment out steps you don't need 😁
        # todo: sync footprints to nodes.
        step0_staging()        
        # todo: rsync staging to /scratch
        # todo: merge staged files in /scratch
        # DO NOT RUN 3d-tiling UNTIL WORKFLOW CAN ACCOMODATE FILE HIERARCHY:step1_3d_tiles() # default parameter batch_size = 300 
        # step2_raster_highest(batch_size=100) # rasterize highest Z level only
        # todo: immediately after initiating above step, start rsync script to continuously sync geotiff files,
        # or immediately after the above step is done, rsync all files at once if there is time left in job  
        # step3_raster_lower(batch_size_geotiffs=100) # rasterize all LOWER Z levels
        # todo: immediately after initiating above step, start rsync script to continuously sync geotiff files,
        # or immediately after the above step is done, rsync all files at once if there is time left in job
        # step4_webtiles(batch_size_web_tiles=250) # convert to web tiles.
        
        # mem_testing = False        
        # if mem_testing:
        #     from pympler import tracker
        #     tr = tracker.SummaryTracker()
        #     tr.print_diff()
        #     step2_raster_highest(batch_size=100)                # rasterize highest Z level only 
        #     tr.print_diff()        

    except Exception as e:
        print(f"Caught error in main(): {str(e)}", "\nTraceback", traceback.print_exc())
    finally:
        # cancel work. shutdown ray.
        print(f"Runtime: {(time.time() - start)/60:.2f} minutes")
        ray.shutdown()
        assert ray.is_initialized() == False

############### 👇 MAIN STEPS FUNCTIONS 👇 ###############

# @workflow.step(name="Step0_Stage_All")
def step0_staging():
    FAILURES = []
    FAILURE_PATHS = []
    IP_ADDRESSES_OF_WORK = []
    app_futures = []
    start = time.time()

    # update the config for the current context: write staged files to local /tmp dir
    IWP_CONFIG['dir_staged'] = IWP_CONFIG['dir_staged_local']
    # make directories in /scratch so geotiffs can populate there
    # is the following line necessary? I don't think so
    os.makedirs(IWP_CONFIG['dir_staged'], exist_ok = True)
    
    # OLD METHOD "glob" all files. 
    stager = pdgstaging.TileStager(IWP_CONFIG, check_footprints=False)
    missing_footprints = stager.check_footprints()
    print(f"⚠️ Num missing footprints: {len(missing_footprints)}")
    
    # define the input dir as the base dir for the satging step
    stager.tiles.add_base_dir('base_input', IWP_CONFIG['dir_input'], '.shp')
    staging_input_files_list = stager.tiles.get_filenames_from_dir(base_dir = 'base_input')
    
    # write list staging_input_files_list to file to local /tmp staged dir
    # before we transfer all staged files and other staging summary files to /scratch 
    with open(os.path.join(IWP_CONFIG['dir_staged'], "staging_input_files_list.json") , "w") as f:
        json.dump(staging_input_files_list, f)
    
    """
    USING FILE LIST, vs GLOB. 
    # Input files! Now we use a list of files ('iwp-file-list.json')
    try:
        staging_input_files_list_raw = json.load(open('./iwp-file-list.json'))
    except FileNotFoundError as e:
        print("Hey you, please specify a json file containing a list of input file paths (relative to `BASE_DIR_OF_INPUT`).", e)
    
    if ONLY_SMALL_TEST_RUN: # for testing only
        staging_input_files_list_raw = staging_input_files_list_raw[:TEST_RUN_SIZE]
    
    # make paths absolute 
    staging_input_files_list = prepend(staging_input_files_list_raw, BASE_DIR_OF_INPUT)
    
    """
    
    # catch kill signal to shutdown on command (ctrl + c)
    try: 
        # batch_size = 5
        # staged_batches = make_batch(staging_input_files_list, batch_size)
        
        # START all jobs
        print(f"\n\n👉 Staging {len(staging_input_files_list)} files in parallel 👈\n\n")
        for itr, filepath in enumerate(staging_input_files_list):    
            # if itr <= 6075:
            # create list of remote function ids (i.e. futures)
            app_futures.append(stage_remote.remote(filepath))

        # COLLECT all jobs as they finish.
        # BLOCKING - WAIT FOR *ALL* REMOTE FUNCTIONS TO FINISH
        for i in range(0, len(app_futures)): 
            # ray.wait(app_futures) catches ONE at a time. 
            ready, not_ready = ray.wait(app_futures) # todo , fetch_local=False do not download object from remote nodes

            # check for failures
            if any(err in ray.get(ready)[0][0] for err in ["FAILED", "Failed", "❌"]):
                FAILURES.append([ray.get(ready)[0][0], ray.get(ready)[0][1]])
                print(f"❌ Failed {ray.get(ready)[0][0]}")
                IP_ADDRESSES_OF_WORK.append(ray.get(ready)[0][1])
            else:
                # success case
                print(f"✅ Finished {ray.get(ready)[0][0]}")
                print(f"📌 Completed {i+1} of {len(staging_input_files_list)}, {(i+1)/len(staging_input_files_list)*100:.2f}%, ⏰ Elapsed time: {(time.time() - start)/60:.2f} min.\n🔮 Estimated total runtime: { ((time.time() - start)/60) / ((i+1)/len(staging_input_files_list)) :.2f} minutes.\n")
                IP_ADDRESSES_OF_WORK.append(ray.get(ready)[0][1])

            app_futures = not_ready
            if not app_futures:
                break
    except Exception as e:
        print(f"Cauth error in Staging (step_0): {str(e)}")
    finally:
        # print FINAL stats about Staging 
        print(f"Runtime: {(time.time() - start)/60:.2f} minutes\n")        
        for failure in FAILURES: print(failure) # for pretty-print newlines
        print(f"Number of failures = {len(FAILURES)}")
        print("Which nodes were used?")
        for ip_address, num_tasks in Counter(IP_ADDRESSES_OF_WORK).items():
            print('    {} tasks on {}'.format(num_tasks, ip_address))
        return "😁 step 1 success"

# todo: refactor for a uniform function to check for failures
def check_for_failures():
    # need to append to FAILURES list and FAILURE_PATHS list, and IP_ADDRESSES_OF_WORK list
    pass

def print_cluster_stats():
    print("Querying size of Ray cluster...\n")

    # print at start of staging
    print(f'''This cluster consists of
        {len(ray.nodes())} nodes in total
        {ray.cluster_resources()['CPU']} CPU cores in total
        {ray.cluster_resources()['memory']/1e9:.2f} GB CPU memory in total''')
    if ('GPU' in str(ray.cluster_resources())):
        print(f"        {ray.cluster_resources()['GPU']} GRAPHICCSSZZ cards in total")

def prep_only_high_ice_input_list():
    try:
        staging_input_files_list_raw = json.load(open('./iwp-file-list.json'))
    except FileNotFoundError as e:
        print("❌❌❌ Hey you, please specify a 👉 json file containing a list of input file paths 👈 (relative to `BASE_DIR_OF_INPUT`).", e)

    if ONLY_SMALL_TEST_RUN: # for testing only
        staging_input_files_list_raw = staging_input_files_list_raw[:TEST_RUN_SIZE]

    # make paths absolute (not relative) 
    staging_input_files_list = prepend(staging_input_files_list_raw, IWP_CONFIG['dir_input'])
    
    # ONLY use high_ice for a test run.
    print("⚠️ WARNING: ONLY USING HIGH_ICE FOR A TEST RUN!!!!!  ⚠️")
    print("Length before filter: ", len(staging_input_files_list))
    staging_input_files_list = list( filter(lambda filepath_str: any(keep_str in filepath_str for keep_str in ['high_ice/alaska', 'water_clipped/alaska']), staging_input_files_list) )
    print("Length after filter: ", len(staging_input_files_list))
    
    return staging_input_files_list

def prepend(mylist, mystr):
    '''
    Prepend str to front of each element of List. Typically filepaths.
    '''
    return [mystr + item for item in mylist]

# @workflow.step(name="Step1_3D_Tiles")
# def step1_3d_tiles(batch_size=300):
    
#     # instantiate classes for their helper functions
#     # rasterizer = pdgraster.RasterTiler(IWP_CONFIG)
#     stager = pdgstaging.TileStager(IWP_CONFIG, check_footprints=False)
    
#     IP_ADDRESSES_OF_WORK_3D_TILES = []
#     FAILURES_3D_TILES = []
    
#     print("1️⃣  Step 1 3D Tiles from staged files.")

#     try:
#         # collect staged files from STAGING_REMOTE dir
#         # first define the dir that contains the staged files into a base dir to pull all filepaths correctly
#         stager.tiles.add_base_dir('staging', IWP_CONFIG['dir_staged_remote'], '.gpkg')
#         staged_files_list = stager.tiles.get_filenames_from_dir(base_dir = 'staging')
#         staged_files_list.reverse()
        
#         staged_batches = make_batch(staged_files_list, batch_size)

#         if ONLY_SMALL_TEST_RUN:
#             staged_files_list = staged_files_list[:TEST_RUN_SIZE]
#         print("total staged files len:", len(staged_files_list))

#         # START JOBS
#         start = time.time()
#         app_futures = []
#         for itr, filepath_batch in enumerate(staged_batches):
#             # check if file is already done
#             # filename, save_to_dir = build_filepath(filepath)
            
#             # check_if_exists = False
#             # if check_if_exists:
#             #     filename, save_to_dir = build_step1_3d_filepath(OUTPUT, filepath)
#             #     absolute_path = os.path.join(save_to_dir, filename) + ".json"
#             #     if os.path.exists(absolute_path):
#             #         print(f"Output already exists, skipping: {absolute_path}")
#             #         continue
#             #     else:
#             #         print(f"Starting job ({itr}/{len(staged_files_list)}): {absolute_path}")
#             #         app_futures.append(three_d_tile.remote(filepath, filename, save_to_dir))
            
#             # batched version        
#             app_futures.append(three_d_tile_batches.remote(filepath_batch, IWP_CONFIG['dir_output']))

#         # get 3D tiles, send to Tileset
#         for i in range(0,len(staged_files_list)): 
#             ready, not_ready = ray.wait(app_futures)
            
#             # Check for failures
#             if any(err in ray.get(ready)[0] for err in ["FAILED", "Failed", "❌"]):
#                 # failure case
#                 FAILURES_3D_TILES.append( [ray.get(ready)[0], ray.get(ready)[0][1]] )
#                 print(f"❌ Failed {ray.get(ready)[0]}")
#             else:
#                 # success case
#                 print(f"✅ Finished {ray.get(ready)[0]}")
#                 print(f"📌 Completed {i+1} of {len(staged_batches)}, {(i+1)/len(staged_batches)*100:.2f}%, ⏰ Elapsed time: {(time.time() - start)/60:.2f} min\n")
#                 IP_ADDRESSES_OF_WORK_3D_TILES.append(ray.get(ready)[0])

#             app_futures = not_ready
#             if not app_futures:
#                 break
            
#     except Exception as e:
#         print("❌❌  Failed in main Ray loop (of Viz-3D). Error:", e)
#     finally:
#         print(f"⏰ Running total of elapsed time: {(time.time() - start)/60:.2f} minutes\n")
#         print("Which nodes were used?")
#         for ip_address, num_tasks in Counter(IP_ADDRESSES_OF_WORK_3D_TILES).items():
#             print('    {} tasks on {}'.format(num_tasks, ip_address))
#         print(f"There were {len(FAILURES_3D_TILES)} failures.")
#         # todo -- group failures by node IP address...
#         for failure_text, ip_address in FAILURES_3D_TILES:
#             print(f"    {failure_text} on {ip_address}")

# @workflow.step(name="Step2_Rasterize_only_higest_z_level")
def step2_raster_highest(batch_size=100):
    # from pympler import tracker
    # tr_within_raster_highest = tracker.SummaryTracker()
    # tr_outside_for_loop = tracker.SummaryTracker()
    """
    TODO: There is a memory leak. Something is not being deleted. This will eventually crash the Ray cluster.
    
    This is a BLOCKING step (but it can run in parallel).
    Rasterize all staged tiles (only highest z-level).
    """
    # import gc
    # from random import randrange
    # from pympler import muppy, summary, tracker

    # update the config for the current context: write geotiff files to local /tmp dir
    IWP_CONFIG['dir_geotiff'] = IWP_CONFIG['dir_geotiff_local']
    # make directories in /scratch so geotiffs can populate there
    # is the following line necessary? I don't think so
    os.makedirs(IWP_CONFIG['dir_geotiff'], exist_ok = True)

    print("2️⃣  Step 2 Rasterize only highest Z")

    # update the config for the current context: pull merged staged files from /scratch
    IWP_CONFIG['dir_staged'] = IWP_CONFIG['dir_staged_remote']
    stager = pdgstaging.TileStager(IWP_CONFIG, check_footprints=False)
    # first define the dir that contains the staged files into a base dir to pull all filepaths correctly
    stager.tiles.add_base_dir('output_of_staging', IWP_CONFIG['dir_staged'], '.gpkg')
    
    print(f"Collecting all STAGED files from `{IWP_CONFIG['dir_staged']}`...")
    # Get paths to all the newly staged tiles
    staged_paths = stager.tiles.get_filenames_from_dir(base_dir = 'output_of_staging')
    
    # stager.kas_check_footprints(staged_paths)
    
    # Add footprints path! 
    # todo: not sure this is the right extension.
    # stager.tiles.add_base_dir('input', FOOTPRINTS_PATH, '.shp')

    if ONLY_SMALL_TEST_RUN:
        staged_paths = staged_paths[:TEST_RUN_SIZE]
    
    # save a copy of the files we're rasterizing.
    staged_path_json_filepath = os.path.join(IWP_CONFIG['dir_output'], "staged_paths_to_rasterize_highest.json")
    print(f"Writing a copy of the files we're rasterizing to {staged_path_json_filepath}...")
    with open(staged_path_json_filepath, "w") as f:
        json.dump(staged_paths, f, indent=2)

    print(f"Step 2️⃣ -- Making batches of staged files... batch_size: {batch_size}")
    staged_batches = make_batch(staged_paths, batch_size)

    print(f"The input to this step, Rasterization, is the output of Staging.\n Using Staging path: {IWP_CONFIG['dir_staged']}")

    print(f"🌄 Rasterize total files {len(staged_paths)} gpkgs, using batch size: {batch_size}")
    print(f"🏎  Parallel batches of jobs: {len(staged_batches)}...\n")

    
    # ADD PLACEMENT GROUP FOR ADDED STABILITY WITH MANY NODES
    # print("Creating placement group for Step 2 -- raster highest...")    
    # from ray.util.placement_group import placement_group
    # num_cpu_per_actor = 1
    # pg = placement_group([{"CPU": num_cpu_per_actor}] * NUM_PARALLEL_CPU_CORES, strategy="SPREAD") # strategy="STRICT_SPREAD" strict = only one job per node. Bad. 
    # ray.get(pg.ready()) # wait for it to be ready
    
    print(f"total STAGED filepaths: {len(staged_paths)}.")
    print("total filepath batches: ", len(staged_batches), "<-- total number of jobs to submit")
    logging.info(f"total STAGED filepaths (for raster_highest()): {len(staged_paths)}.")
    logging.info(f"total STAGED filepath batches (for raster_highest()): {len(staged_batches)} <-- total number of jobs to submit")
    
    start = time.time()
    
    # tr_outside_for_loop.print_diff()
    
    # catch kill signal to shutdown on command (ctrl + c)
    try: 
        # Start remote functions
        app_futures = []
        for i, batch in enumerate(staged_batches):
            app_future = rasterize.remote(batch, IWP_CONFIG)
            app_futures.append(app_future)
                
        for i in range(0, len(app_futures)): 
            ready, not_ready = ray.wait(app_futures)
            # print(f"✅ Finished {ray.get(ready)}")
            print(f"📌 Completed {i+1} of {len(staged_batches)}")
            print(f"⏰ Running total of elapsed time: {(time.time() - start)/60:.2f} minutes\n🔮 Estimated total runtime: { ((time.time() - start)/60) / ((i+1)/len(staged_batches)) :.2f} minutes.\n")

            # todo: memory debugging
            # tr_within_raster_highest.print_diff()
            # print(f"☝️ Inside ray app_future collection loop {i}")
            
            # snapshot = tracemalloc.take_snapshot()
            # print("========== SNAPSHOT =============")
            # for stat in snapshot.statistics("lineno"):
            #     print(stat)
            #     print(stat.traceback.format())
            # # TODO: remote this...
            # for handler in logger.handlers:
            #     handler.close()
            # TODO: maybe remove
            # clean up since Ray doesn't do much garbage collection.
            # gc.collect()
            # print(gc.get_stats())
            
            app_futures = not_ready
            if not app_futures:
                break
    except Exception as e:
        print(f"Cauth error in Ray loop: {str(e)}")
    finally:
        # shutdown ray 
        end = time.time()
        print(f"Runtime: {(end - start):.2f} seconds")
        ray.shutdown()
        assert ray.is_initialized() == False
    
    # tr_outside_for_loop.print_diff()
    
    print(f'⏰ Total time to rasterize {len(staged_paths)} tiles: {(time.time() - start)/60:.2f} minutes\n')
    return "Done rasterize all only highest z level"

# @workflow.step(name="Step3_Rasterize_lower_z_levels")
def step3_raster_lower(batch_size_geotiffs=20):
    '''
    STEP 3: Create parent geotiffs for all z-levels (except highest)
    THIS IS HARD TO PARALLELIZE multiple zoom levels at once..... sad, BUT
    👉 WE DO PARALLELIZE BATCHES WITHIN one zoom level.
    '''
    print("3️⃣ Step 3: Create parent geotiffs for all lower z-levels (everything except highest zoom)")

    # update the config for the current context: pull stager that represents staged files in /scratch
    # not sure that this next line is necessary but might as well keep the config up to date
    # with where files currently are
    IWP_CONFIG['dir_staged'] = IWP_CONFIG['dir_staged_remote']
    stager = pdgstaging.TileStager(IWP_CONFIG, check_footprints=False)
    
    # find all Z levels
    min_z = stager.config.get_min_z()
    max_z = stager.config.get_max_z()
    parent_zs = range(max_z - 1, min_z - 1, -1)

    # update the config for the current context: pull highest rasters from /scratch
    IWP_CONFIG['dir_geotiff'] = IWP_CONFIG['dir_geotiff_remote']

    print(f"Collecting all Geotiffs (.tif) in: {IWP_CONFIG['dir_geotiff']}")
    stager.tiles.add_base_dir('geotiff_path', IWP_CONFIG['dir_geotiff'], '.tif') # already added ??

    start = time.time()
    # Can't start lower z-level until higher z-level is complete.
    for z in parent_zs:
        print(f"👉 Starting Z level {z} of {len(parent_zs)}")
        # Loop thru Z levels 
        # Make lower z-levels based on the path names of the files just created
        child_paths = stager.tiles.get_filenames_from_dir(base_dir = 'geotiff_path', z=z + 1)

        if ONLY_SMALL_TEST_RUN:
            child_paths = child_paths[:TEST_RUN_SIZE]

        parent_tiles = set()
        for child_path in child_paths:
            parent_tile = stager.tiles.get_parent_tile(child_path)
            parent_tiles.add(parent_tile)
        parent_tiles = list(parent_tiles)

        # Break all parent tiles at level z into batches
        parent_tile_batches = make_batch(parent_tiles, batch_size_geotiffs)
        print(f"📦 Rasterizing {len(parent_tiles)} parents into {len(child_paths)} children tifs (using {len(parent_tile_batches)} batches)")
        # print(f"📦 Rasterizing {parent_tile_batches} parents")

        # now that geotiff_path has been created as a base dir and batched, switch the dir_geotiff to _local
        # so that new raster lower files are written to /tmp rather than /scratch
        IWP_CONFIG['dir_geotiff'] = IWP_CONFIG['dir_geotiff_local']
        # no need to set rasterizer with new config after chaning this property cause 
        # that is done within the function create_composite_geotiffs() which is called
        # in next loop

        # PARALLELIZE batches WITHIN one zoom level (best we can do for now).
        try:
            app_futures = []
            for parent_tile_batch in parent_tile_batches:
                # MANDATORY: include placement_group for better stability on 200+ cpus.
                app_future = create_composite_geotiffs.remote(parent_tile_batch, IWP_CONFIG, logging_dict=None)
                app_futures.append(app_future)

            # Don't start the next z-level (or move to step 4) until the
            # current z-level is complete
            for i in range(0, len(app_futures)): 
                ready, not_ready = ray.wait(app_futures)
                print(f"✅ Finished {ray.get(ready)}")
                print(f"📌 Completed {i+1} of {len(parent_tile_batches)}, {(i+1)/len(parent_tile_batches)*100:.2f}%")
                print(f"⏰ Running total of elapsed time: {(time.time() - start)/60:.2f} minutes. 🔮 Estimated time to completion: { ((time.time() - start)/60) / ((i+1)/len(parent_tile_batches)) :.2f} minutes.\n")
                app_futures = not_ready
                if not app_futures:
                    break
        except Exception as e:
            print(f"Cauth error in Ray loop in step 3: {str(e)}")

    print(f"⏰ Total time to create parent geotiffs: {(time.time() - start)/60:.2f} minutes\n")
    return "Done step 3."

# @workflow.step(name="Step4_create_webtiles")
def step4_webtiles(batch_size_web_tiles=300):
    '''
    STEP 4: Create web tiles from geotiffs
    Infinite parallelism.
    '''
    print("4️⃣ -- Creating web tiles from geotiffs...")
    
    # instantiate classes for their helper functions
    # not sure that the next line is necessary but might as well keep the config up to date
    # with where files currently are
    IWP_CONFIG['dir_staged'] = IWP_CONFIG['dir_staged_remote']
    # pull all z-levels of rasters from /scratch for web tiling
    IWP_CONFIG['dir_geotiff'] = IWP_CONFIG['dir_geotiff_remote']
    # define rasterizer here just to updates_ranges()
    rasterizer = pdgraster.RasterTiler(IWP_CONFIG)
    stager = pdgstaging.TileStager(IWP_CONFIG, check_footprints=False)
    
    start = time.time()
    # Update color ranges
    rasterizer.update_ranges()

    # Note: we also define rasterizer latermin each of the 3 functions:
    # raster highest, raster lower, and webtile functions

    print(f"Collecting all Geotiffs (.tif) in: {IWP_CONFIG['dir_geotiff']}...")
    stager.tiles.add_base_dir('geotiff_path', IWP_CONFIG['dir_geotiff'], '.tif')
    geotiff_paths = stager.tiles.get_filenames_from_dir(base_dir = 'geotiff_path')

    if ONLY_SMALL_TEST_RUN:
        geotiff_paths = geotiff_paths[:TEST_RUN_SIZE]
    
    geotiff_batches = make_batch(geotiff_paths, batch_size_web_tiles)
    print(f"📦 Creating web tiles from geotiffs. Num parallel batches = {len(geotiff_batches)}")
    
    # ADD PLACEMENT GROUP FOR ADDED STABILITY WITH MANY NODES
    # print("Creating placement group for Step 2 -- raster highest")    
    # from ray.util.placement_group import placement_group
    # num_cpu_per_actor = 1
    # pg = placement_group([{"CPU": num_cpu_per_actor}] * NUM_PARALLEL_CPU_CORES, strategy="SPREAD") # strategy="STRICT_SPREAD" strict = only one job per node. Bad. 
    # ray.get(pg.ready()) # wait for it to be ready
    
    # catch kill signal to shutdown on command (ctrl + c)
    try: 
        # Start remote functions
        app_futures = []
        for batch in geotiff_batches:
            # MANDATORY: include placement_group for better stability on 200+ cpus.
            # app_future = create_web_tiles.options(placement_group=pg).remote(batch, IWP_CONFIG)
            app_future = create_web_tiles.remote(batch, IWP_CONFIG)
            app_futures.append(app_future)

        for i in range(0, len(app_futures)): 
            ready, not_ready = ray.wait(app_futures)
            print(f"✅ Finished {ray.get(ready)}")
            print(f"📌 Completed {i+1} of {len(geotiff_batches)}")
            print(f"⏰ Running total of elapsed time: {(time.time() - start)/60:.2f} minutes\n")
            app_futures = not_ready
            if not app_futures:
                break
    except Exception as e:
        print(f"Cauth error in Ray loop: {str(e)}")
    finally:
        # cancel work. shutdown ray 
        end = time.time()
        print(f"Runtime: {(end - start):.2f} seconds")

    print(f'⏰ Total time to create web tiles:  {len(geotiff_paths)} tiles: {(time.time() - start)/60:.2f} minutes\n')

############### HELPER FUNCTIONS BELOW HERE ###############

@ray.remote
def rasterize(staged_paths, config, logging_dict=None):
    """
    Rasterize a batch of vector files (step 2)
    """
    # config['dir_input'] = OUTPUT_OF_STAGING

    # if logging_dict:
    #     import logging.config
    #     logging.config.dictConfig(logging_dict)
    # print(staged_paths)

    try:
        rasterizer = pdgraster.RasterTiler(config)
        # from pympler.classtracker import ClassTracker
        # tracker = ClassTracker()
        # tracker.track_object(rasterizer, resolution_level=4) # todo try resolution 2
        # tracker.track_class(pdgraster.RasterTiler, resolution_level=4)
        # tracker.create_snapshot()
        # todo: fix warning `python3.9/site-packages/geopandas/base.py:31: UserWarning: The indices of the two GeoSeries are different.`
        # with suppress(UserWarning): 
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            rasterizer.rasterize_vectors(staged_paths, make_parents=False)
        # tracker.create_snapshot()
        # tracker.stats.print_summary()
    except Exception as e:
        print("⚠️ Failed to rasterize path: ", staged_paths, "\nWith error: ", e, "\nTraceback", traceback.print_exc())
    finally:
        # MEMORY LEAK (related to loggers being created)
        # for handler in logger.handlers:
        #     handler.close()
        # del rasterizer
        # tracker.create_snapshot()
        # tracker.stats.print_summary()
        # print("After deleting rasterizer")
        return staged_paths

@ray.remote
def create_composite_geotiffs(tiles, config, logging_dict=None):
    """
    Make composite geotiffs (step 3)
    """
    if logging_dict:
        import logging.config
        logging.config.dictConfig(logging_dict)
    try:
        rasterizer = pdgraster.RasterTiler(config)
        rasterizer.parent_geotiffs_from_children(tiles, recursive=False)
    except Exception as e:
        print("⚠️ Failed to create rasterizer. With error ", e)
    return 0

@ray.remote
def create_web_tiles(geotiff_paths, config, logging_dict=None):
    """
    Create a batch of webtiles from geotiffs (step 4)
    """
    import pdgraster
    if logging_dict:
        import logging.config
        logging.config.dictConfig(logging_dict)
    try:
        rasterizer = pdgraster.RasterTiler(config)
        rasterizer.webtiles_from_geotiffs(geotiff_paths, update_ranges=False)
    except Exception as e:
        print("⚠️ Failed to create webtiles. With error ", e)
    return 0

def make_batch(items, batch_size):
    """
    Simple helper.
    Create batches of a given size from a list of items.
    """
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

@ray.remote
def stage_remote(filepath):
    """
    Step 1. 
    Parallelism at the per-shape-file level.
    """
    # deepcopy makes a realy copy, not using references. Helpful for parallelism.
    # config_path = deepcopy(IWP_CONFIG)
    
    try: 
        stager = pdgstaging.TileStager(config=IWP_CONFIG, check_footprints=False)
        ret = stager.stage(filepath)
        if 'Skipping' in str(ret):
            print(f"⚠️ Skipping {filepath}")
    except Exception as e:
        return [filepath,
                socket.gethostbyname(socket.gethostname()),
                "‼️ ‼️ ❌ ❌ ❌ ❌ -- THIS TASK FAILED ❌ ❌ ❌ ❌ ‼️ ‼️"]
        
    # return [input_path, host_computer]
    return [filepath, 
            socket.gethostbyname(socket.gethostname())]

# 🎯 Best practice to ensure unique Workflow names.
def make_workflow_id(name: str) -> str:
    from datetime import datetime

    import pytz

    # Timezones: US/{Pacific, Mountain, Central, Eastern}
    # All timezones `pytz.all_timezones`. Always use caution with timezones.
    curr_time = datetime.now(pytz.timezone('US/Central'))
    return f"{name}-{str(curr_time.strftime('%h_%d,%Y@%H:%M'))}"

def build_filepath(input_file):
    # Demo input: /home/kastanday/output/staged/WorldCRS84Quad/13/1047/1047.gpkg

    # Replace 'staged' -> '3d_tiles' in path
    # In:       /home/kastanday/output/staged/WorldCRS84Quad/13/1047/1047.gpkg
    # new_path: /home/kastanday/output/3d_tiles/WorldCRS84Quad/13/1047/1047.gpkg
    path = pathlib.Path(input_file)
    index = path.parts.index('staged')
    pre_path = os.path.join(*path.parts[:index])
    post_path = os.path.join(*path.parts[index+1:])
    new_path = os.path.join(pre_path, '3d_tiles', post_path)
    new_path = pathlib.Path(new_path)

    # filename: 1047
    filename = new_path.stem

    # save_to_dir: /home/kastanday/output/staged/WorldCRS84Quad/13/1047
    save_to_dir = new_path.parent

    return str(filename), str(save_to_dir)

def build_step1_3d_filepath(outpit_dir, input_file):
    '''
    Usage: 
        input_file = f'/home/{user}/output/staged/WorldCRS84Quad/13/1047/104699.gpkg'
        output_dir = '/temp/v3_viz_output/'
        
    Returns:
        ('104699', '/temp/v3_viz_output/3d_tiles/WorldCRS84Quad/13/1047')
    
    '''
    # Demo input: /home/kastanday/output/staged/WorldCRS84Quad/13/1047/1047.gpkg

    # Replace 'staged' -> '3d_tiles' in path
    # In:       /home/kastanday/output/staged/WorldCRS84Quad/13/1047/1047.gpkg
    # new_path: /home/kastanday/output/3d_tiles/WorldCRS84Quad/13/1047/1047.gpkg
    path = pathlib.Path(input_file)
    index = path.parts.index('staged')
    pre_path = os.path.join(*path.parts[:index])
    post_path = os.path.join(*path.parts[index+1:])
    new_path = os.path.join(outpit_dir, '3d_tiles', post_path) #todo
    new_path = pathlib.Path(new_path)

    # filename: 1047
    filename = new_path.stem

    # save_to_dir: /home/kastanday/output/staged/WorldCRS84Quad/13/1047
    save_to_dir = new_path.parent

    return str(filename), str(save_to_dir)

@ray.remote
def three_d_tile(input_file, filename, save_to):
    
    ## todo: there is optimization here. 
    # todo: refactor this to check if the output exists first thing (save metadata ops):  tileset.set_json_filename(filename)

    try:
        # Check if the output exists. If so, skip. Do this in outer loop (less ray tasks to make)
        # absolute_path = save_to + filename + ".json"
        # if os.path.exists(absolute_path):
        #     print(f"Output already exists, skipping: {absolute_path}")
        #     return 0
        
        
        # the 3DTile saves the .b3dm file. 
        tile = viz_3dtiles.Cesium3DTile()
        tile.set_save_to_path(save_to)
        tile.set_b3dm_name(filename)
        # build 3d tile.
        tile.from_file(filepath=input_file)

        # Tileset saves the json summary.
        tileset = viz_3dtiles.Cesium3DTileset(tile)
        tileset.set_save_to_path(save_to)
        tileset.set_json_filename(filename)
        tileset.write_file()
        
    except Exception as e:
        return [f"‼️ ‼️ ❌ ❌ ❌ ❌ -- THIS TASK FAILED ❌ ❌ ❌ ❌ ‼️ ‼️ with path: {save_to, filename} and \nError: {e}\nTraceback: {traceback.print_exc()}", 
    socket.gethostbyname(socket.gethostname())]
    
    # success case
    return [f"Path {save_to}", socket.gethostbyname(socket.gethostname())]

@ray.remote
def three_d_tile_batches(file_batch, output_dir):
    
    ## todo: there is optimization here. 
    # todo: refactor this to check if the output exists first thing (save metadata ops):  tileset.set_json_filename(filename)

    # Check if the output exists. If so, skip. Do this in outer loop (less ray tasks to make)
    # absolute_path = save_to + filename + ".json"
    
    # todo: use robyn's new 3d-tiles code. 
    
    for filepath in file_batch:
        try:
            # build final path & check if output exists
            filename, save_to_dir = build_step1_3d_filepath(output_dir, filepath)
            absolute_path = os.path.join(save_to_dir, filename) + ".json"
            if os.path.exists(absolute_path):
                print(f"Output already exists, skipping: {absolute_path}")
                continue
        
            # the 3DTile saves the .b3dm file. 
            tile = viz_3dtiles.Cesium3DTile()
            tile.set_save_to_path(save_to_dir)
            tile.set_b3dm_name(filename)
            # build 3d tile.
            tile.from_file(filepath=filepath)

            # Tileset saves the json summary.
            tileset = viz_3dtiles.Cesium3DTileset(tile)
            tileset.set_save_to_path(save_to_dir)
            tileset.set_json_filename(filename)
            tileset.write_file()
            print("Success on 3d tile")
        
        except Exception as e:
            print(f"‼️ ‼️ ❌ ❌ ❌ ❌ -- THIS TASK FAILED ❌ ❌ ❌ ❌ ‼️ ‼️ with path (some part of this list): {filepath} and \nError: {e}\nTraceback: {traceback.print_exc()}", \
                socket.gethostbyname(socket.gethostname()))
        
    # success case
    return [f"Path {file_batch}", socket.gethostbyname(socket.gethostname())]

def start_logging():
    '''
    In output directory. 
    '''
    filepath = pathlib.Path(IWP_CONFIG['dir_output'] + 'workflow_log.txt')
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.touch(exist_ok=True)
    logging.basicConfig(level=logging.INFO, filename= IWP_CONFIG['dir_output'] + 'workflow_log.txt', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == '__main__':
    main()

