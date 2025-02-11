
import json

import logging
import logging.handlers

import os
import pathlib
import socket  # for seeing where jobs run when distributed
import sys
import time
import traceback
import warnings
#from asyncio.log import logger
from collections import Counter
from contextlib import suppress
from copy import deepcopy
from datetime import datetime
import subprocess
import pprint
import shlex

import pdgraster
import pdgstaging 
import ray

# define user on Delta, avoid writing files to other user's dir
user = subprocess.check_output("whoami").strip().decode("ascii")

import infrastructure_config
CONFIG = infrastructure_config.CONFIG

# configure logger
logger = logging.getLogger("logger")
# Remove any existing handlers from the logger
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
# prevent logging statements from being printed to terminal
logger.propagate = False
# set up new handler
handler = logging.FileHandler("/tmp/log.log")
formatter = logging.Formatter(logging.BASIC_FORMAT)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def main():
    result = subprocess.run(["hostname", "-i"], capture_output=True, text=True)
    head_ip = result.stdout.strip()
    #print("connecting to ray.")
    #ray.init(address='auto', dashboard_port=8265)
    print(f"Connecting to Ray... at address ray://{head_ip}:10001")
    ray.init(address=f'ray://{head_ip}:10001', dashboard_port=8265)   # most reliable way to start Ray
    # use port-forwarding to see dashboard: `ssh -L 8265:localhost:8265 kastanday@kingfisher.ncsa.illinois.edu`
    assert ray.is_initialized() == True
    print("🎯 Ray initialized.")
    
    print_cluster_stats()
    start = time.time()
    
    try:
        ########## MAIN STEPS ##########
        print("Starting main...")
        
        # (optionally) Comment out steps you don't need 😁
        # todo: sync footprints to nodes.
        # step0_staging()
        # todo: rsync staging to /scratch
        # todo: merge staged files in /scratch    # ./merge_staged_vector_tiles.py
        # DO NOT RUN 3d-tiling UNTIL WORKFLOW CAN ACCOMODATE FILE HIERARCHY:step1_3d_tiles() # default parameter batch_size = 300 
        # step2_raster_highest(batch_size=100) # rasterize highest Z level only
        # todo: immediately after initiating above step, start rsync script to continuously sync geotiff files,
        # or immediately after the above step is done, rsync all files at once if there is time left in job  
        # step3_raster_lower(batch_size_geotiffs=100) # rasterize all LOWER Z levels
        step4_webtiles(batch_size_web_tiles=250) # convert to web tiles.       

    except Exception as e:
        print(f"Caught error in main(): {str(e)}", "\nTraceback", traceback.print_exc())
    finally:
        # cancel work. shutdown ray.
        print(f"Runtime: {(time.time() - start)/60:.2f} minutes")
        ray.shutdown()
        assert ray.is_initialized() == False

############### 👇 MAIN STEPS FUNCTIONS 👇 ###############

def step0_staging():
    FAILURES = []
    FAILURE_PATHS = []
    IP_ADDRESSES_OF_WORK = []
    app_futures = []
    start = time.time()

    # test logging configuration
    logger.info("step0_staging() has initiated.")

    # update the config for the current context: write staged files to local /tmp dir
    config = deepcopy(CONFIG)
    config['dir_staged'] = config['dir_staged_local']   
    # make directory /tmp/staged on each node
    # not really necessary cause functions are set up to do this
    # and /tmp allows dirs to be created to write files
    os.makedirs(config['dir_staged'], exist_ok = True)
    
    stager = pdgstaging.TileStager(config, check_footprints=False)
    
    staging_input_files_list = stager.tiles.get_filenames_from_dir('input')

    # record number of filepaths before they are converted into app_futures to detemrine if that's where the bug is
    # with open(os.path.join(iwp_config['dir_output'], "workflow_log.txt"), "a+") as file:
    #     file.write(f"Number of filepaths in staging_input_files_list: {len(staging_input_files_list)}\n\n")   # removed this because trying to get logger to work with special config to log.log rather than Kastan's file
     
    with open(os.path.join(config['dir_output'], "staging_input_files_list.json") , "w") as f:
        json.dump(staging_input_files_list, f)
    
    """
    USING FILE LIST, vs GLOB. 
    # Input files! Now we use a list of files ('iwp-file-list.json')
    try:
        staging_input_files_list_raw = json.load(open('./iwp-file-list.json'))
    except FileNotFoundError as e:
        print("Hey you, please specify a json file containing a list of input file paths (relative to `BASE_DIR_OF_INPUT`).", e)
    
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
            app_futures.append(stage_remote.remote(filepath, config))
        
        # record how many app_futures were created to determine if it is the full number of input paths
        # with open(os.path.join(iwp_config['dir_output'], "workflow_log.txt"), "a+") as file:
        #     file.write(f"Number of future staged files: {len(app_futures)}. This is {len(staging_input_files_list)-len(app_futures)} fewer than the original number of files input.\n\n")       # removed this because trying to get logger to work with special config to log.log rather than Kastan's file

        # COLLECT all jobs as they finish.
        # BLOCKING - WAIT FOR *ALL* REMOTE FUNCTIONS TO FINISH
        for i in range(0, len(app_futures)): 
            # ray.wait(app_futures) catches ONE at a time. 
            ready, not_ready = ray.wait(app_futures) # todo , fetch_local=False do not download object from remote nodes

            # check for failures
            if any(err in ray.get(ready)[0][0] for err in ["FAILED", "Failed", "❌"]):
                FAILURES.append([ray.get(ready)[0][0], ray.get(ready)[0][1]])
                print(f"❌ Failed {ray.get(ready)[0][0]}")

                # with open(os.path.join(iwp_config['dir_output'], "workflow_log.txt"), "a+") as file:
                #     file.write(f"Number of FAILURES in preparing future staged files: {len(FAILURES)}\n\n") # removed this because trying to get logger to work with special config to log.log rather than Kastan's file

                IP_ADDRESSES_OF_WORK.append(ray.get(ready)[0][1])
            else:
                # success case
                print(f"✅ Finished {ray.get(ready)[0][0]}")
                print(f"📌 Completed {i+1} of {len(staging_input_files_list)}, {(i+1)/len(staging_input_files_list)*100:.2f}%, ⏰ Elapsed time: {(time.time() - start)/60:.2f} min.\n🔮 Estimated total runtime: { ((time.time() - start)/60) / ((i+1)/len(staging_input_files_list)) :.2f} minutes.\n")

                # with open(f"{iwp_config['dir_output']}workflow_log.txt", "a+") as file:
                #     file.write(f"Success! Finished staging for: {ray.get(ready)[0][0]}\n\n") # removed this because trying to get logger to work with special config to log.log rather than Kastan's file

                IP_ADDRESSES_OF_WORK.append(ray.get(ready)[0][1])

            app_futures = not_ready
            if not app_futures:
                break
    except Exception as e:
        print(f"Cauth error in Staging (step_0): {str(e)}")

        # with open(os.path.join(iwp_config['dir_output'], "workflow_log.txt"), "a+") as file:
        #     file.write(f"ERROR IN STAGING STEP for: {ray.get(ready)[0][0]} with {str(e)}\n\n") # removed this because trying to get logger to work with special config to log.log rather than Kastan's file
        
    finally:
        # print FINAL stats about Staging 
        print(f"Runtime: {(time.time() - start)/60:.2f} minutes\n")

        for failure in FAILURES: print(failure) # for pretty-print newlines
        print(f"Number of failures = {len(FAILURES)}")
        print("Which nodes were used?")
        for ip_address, num_tasks in Counter(IP_ADDRESSES_OF_WORK).items():
            print('    {} tasks on {}'.format(num_tasks, ip_address))

        # with open(os.path.join(iwp_config['dir_output'], "workflow_log.txt"), "a+") as file:
        #     file.write(f"Completed Staging.\n\n") # removed this because trying to get logger to work with special config to log.log rather than Kastan's file

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
        print("❌❌❌ Specify a json file containing a list of input file paths.", e)

    #if ONLY_SMALL_TEST_RUN: # for testing only
    #    staging_input_files_list_raw = staging_input_files_list_raw[:TEST_RUN_SIZE]

    # make paths absolute (not relative) 
    staging_input_files_list = prepend(staging_input_files_list_raw, CONFIG['dir_input'])
    
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
#     # rasterizer = pdgraster.RasterTiler(iwp_config)
#     stager = pdgstaging.TileStager(iwp_config, check_footprints=False)
    
#     IP_ADDRESSES_OF_WORK_3D_TILES = []
#     FAILURES_3D_TILES = []
    
#     print("1️⃣  Step 1 3D Tiles from staged files.")

#     try:
#         # collect staged files from STAGING_REMOTE dir
#         # first define the dir that contains the staged files into a base dir to pull all filepaths correctly
#         stager.tiles.add_base_dir('staging', iwp_config['dir_staged_remote'], '.gpkg')
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
#             app_futures.append(three_d_tile_batches.remote(filepath_batch, iwp_config['dir_output']))

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

    #os.makedirs(iwp_config['dir_geotiff'], exist_ok=True) 

    config = deepcopy(CONFIG)

    print(f"Using config {config}")

    # update the config for the current context: write geotiff files to local /tmp dir
    config['dir_geotiff'] = config['dir_geotiff_local']

    print(f"2️⃣  Step 2 Rasterize only highest Z, saving to {config['dir_geotiff']}")

    config['dir_staged'] = config['dir_staged_remote_merged']
    stager = pdgstaging.TileStager(config, check_footprints=False)
    stager.tiles.add_base_dir('output_of_staging', config['dir_staged'], '.gpkg')
    
    #rasterizer = pdgraster.RasterTiler(iwp_config)
    # make directories in /tmp so geotiffs can populate there
    # is the following line necessary? I don't think so
    #os.makedirs(iwp_config['dir_geotiff'], exist_ok = True)

    # update config for the current context:
    #iwp_config['dir_footprints'] = iwp_config['dir_footprints_local']
    
    # update the config for the current context: pull merged staged files from /scratch
    #iwp_config['dir_staged'] = iwp_config['dir_staged_remote_merged']
    #stager = pdgstaging.TileStager(iwp_config, check_footprints=False)
    # first define the dir that contains the staged files into a base dir to pull all filepaths correctly
    #stager.tiles.add_base_dir('output_of_staging', iwp_config['dir_staged'], '.gpkg')
    
    print(f"Collecting all STAGED files from `{config['dir_staged']}`...")
    # Get paths to all the newly staged tiles
    staged_paths = stager.tiles.get_filenames_from_dir(base_dir = 'output_of_staging')

    #if ONLY_SMALL_TEST_RUN:
    #    staged_paths = staged_paths[:TEST_RUN_SIZE]
    
    # save a copy of the files we're rasterizing.
    staged_path_json_filepath = os.path.join(config['dir_output'], "staged_paths_to_rasterize_highest.json")
    print(f"Writing a copy of the files we're rasterizing to {staged_path_json_filepath}...")
    with open(staged_path_json_filepath, "w") as f:
        json.dump(staged_paths, f, indent=2)

    print(f"Step 2️⃣ -- Making batches of staged files... batch_size: {batch_size}")
    staged_batches = make_batch(staged_paths, batch_size)

    print(f"The input to this step, Rasterization, is the output of Staging.\n Using Staging path: {config['dir_staged']}")

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

            #iwp_config['dir_geotiff'] = iwp_config['dir_geotiff_local']
            #rasterizer = pdgraster.RasterTiler(iwp_config)
            os.makedirs(config['dir_geotiff'], exist_ok=True) 
            app_future = rasterize.remote(batch, config)
            app_futures.append(app_future)
                
        for i in range(0, len(app_futures)): 
            ready, not_ready = ray.wait(app_futures)
            # print(f"✅ Finished {ray.get(ready)}")
            print(f"📌 Completed {i+1} of {len(staged_batches)}")

            # with open(os.path.join(iwp_config['dir_output'], "workflow_log.txt"), "a+") as file:
            #     file.write(f"Raster highest batch success: Completed {i+1} of {len(staged_batches)}\n\n") # removed this because trying to get logger to work with special config to log.log rather than Kastan's file

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
    STEP 3: Create parent geotiffs for all z-levels except highest
    (parallelize batches within one zoom level)
    '''
    print("3️⃣ Step 3: Create parent geotiffs for all lower z-levels (everything except highest zoom)")

    config = deepcopy(CONFIG)

    config['dir_geotiff'] = config['dir_geotiff_remote']
    # update the config for the current context: pull stager that represents staged files in /scratch
    config['dir_staged'] = config['dir_staged_remote_merged']
    stager = pdgstaging.TileStager(config, check_footprints=False)
    
    # find all Z levels
    min_z = stager.config.get_min_z()
    max_z = stager.config.get_max_z()
    parent_zs = range(max_z - 1, min_z - 1, -1)

    config['dir_geotiff'] = config['dir_geotiff_remote']
    rasterizer = pdgraster.RasterTiler(config)

    print(f"Collecting all Geotiffs (.tif) in: {config['dir_geotiff']}")
    # had to change next line from 'geotiff' to 'geotiff_remote' bc got error that the geotiff base dir already existed
    stager.tiles.add_base_dir('geotiff_remote', config['dir_geotiff'], '.tif')

    start = time.time()
    # Can't start lower z-level until higher z-level is complete.
    for z in parent_zs:
        print(f"👉 Starting Z level {z} of {len(parent_zs)}")
        # Loop thru z levels 
        # Make lower z-levels based on the highest z-level rasters
        # removing the next line cause robyn removed it from kastan's script:
        #child_paths = stager.tiles.get_filenames_from_dir(base_dir = 'geotiff_remote', z=z + 1)
        child_paths = stager.tiles.get_filenames_from_dir(base_dir = 'geotiff_remote', z=z + 1)

        #if ONLY_SMALL_TEST_RUN:
        #    child_paths = child_paths[:TEST_RUN_SIZE]

        parent_tiles = set()
        for child_path in child_paths:
            parent_tile = stager.tiles.get_parent_tile(child_path)
            parent_tiles.add(parent_tile)
        parent_tiles = list(parent_tiles)

        # Break all parent tiles at level z into batches
        parent_tile_batches = make_batch(parent_tiles, batch_size_geotiffs)
        # print(f"📦 Rasterizing {len(parent_tiles)} parents into {len(child_paths)} children tifs (using {len(parent_tile_batches)} batches)")
        print(f"📦 Rasterizing (Z-{z}) {len(child_paths)} children into {len(parent_tiles)} parents tifs (using {len(parent_tile_batches)} batches)")
        # print(f"📦 Rasterizing {parent_tile_batches} parents")

        # PARALLELIZE batches WITHIN one zoom level (best we can do for now).
        try:
            app_futures = []
            for parent_tile_batch in parent_tile_batches:
                # even though the geotiff base dir has been created and the filenames have been batched, 
                # still cannot switch the dir_geotiff to _local !!! because the lower z-level rasters need to be
                # written to scratch rather than /tmp so all lower z-levels can access all files in higher z-levels
                #iwp_config['dir_geotiff'] = iwp_config['dir_geotiff_local'] # run immeditely before defining rasterizer
                # I dont think theres a need to set rasterizer with new config after chaning this property cause 
                # that is done within the function create_composite_geotiffs() but troubleshooting 
                # so lets do it anyway
                rasterizer = pdgraster.RasterTiler(config)

                # MANDATORY: include placement_group for better stability on 200+ cpus.
                app_future = create_composite_geotiffs.remote(parent_tile_batch, config, logging_dict=None)
                app_futures.append(app_future)

            # Don't start the next z-level (or move to step 4) until the
            # current z-level is complete
            for i in range(0, len(app_futures)): 
                ready, not_ready = ray.wait(app_futures)
                print(f"✅ Finished {ray.get(ready)}")
                print(f"📌 Completed {i+1} of {len(parent_tile_batches)}, {(i+1)/len(parent_tile_batches)*100:.2f}%")

                # with open(os.path.join(iwp_config['dir_output'], "workflow_log.txt"), "a+") as file:
                #     file.write(f"Raster lower batch success: Completed {i+1} of {len(parent_tile_batches)}\n\n") # removed this because trying to get logger to work with special config to log.log rather than Kastan's file

                print(f"⏰ Running total of elapsed time: {(time.time() - start)/60:.2f} minutes. 🔮 Estimated time to completion: { ((time.time() - start)/60) / ((i+1)/len(parent_tile_batches)) :.2f} minutes.\n")
                app_futures = not_ready
                if not app_futures:
                    break
        except Exception as e:
            print(f"Cauth error in Ray loop in step 3: {str(e)}")

    print(f"⏰ Total time to create parent geotiffs: {(time.time() - start)/60:.2f} minutes\n")
    return "Done step 3."

def step4_webtiles(batch_size_web_tiles=300):
    '''
    STEP 4: Create web tiles from geotiffs
    Infinite parallelism.
    '''
    print("4️⃣ -- Creating web tiles from geotiffs...")
    
    config = deepcopy(CONFIG)

    # instantiate classes for their helper functions
    # not sure that the next line is necessary but might as well keep the config up to date
    # with where files currently are
    config['dir_staged'] = config['dir_staged_remote_merged']
    # pull all z-levels of rasters from /scratch for web tiling
    config['dir_geotiff'] = config['dir_geotiff_remote']
    # define rasterizer here just to updates_ranges()
    rasterizer = pdgraster.RasterTiler(config)
    # stager = pdgstaging.TileStager(iwp_config, check_footprints=False) remove this line, no point in defining the stager right before we update the config, do it after!
    
    start = time.time()
    # Update color ranges
    print(f"Updating ranges...") 
    rasterizer.update_ranges() 
    config_new = rasterizer.config.config 
    print(f"Defined new config: {config_new}") 

    # Note: we also define rasterizer later in each of the 3 functions:
    # raster highest, raster lower, and webtile functions

    # the original config here is fine, just printing filepath for source of geotiff files
    print(f"Collecting all Geotiffs (.tif) in: {config['dir_geotiff']}...") 

    # set add base dir with rasterizer instead of stager
    #rasterizer.tiles.add_base_dir('geotiff_remote', IWP_CONFIG['dir_geotiff'], '.tif')
    #geotiff_paths = rasterizer.tiles.get_filenames_from_dir(base_dir = 'geotiff_remote')
    rasterizer.tiles.add_base_dir('geotiff_remote_all_zs', config_new['dir_geotiff'], '.tif') # call it something different than geotiff_remote because we already made that base dir earlier and it might not overwrite and might error cause already exists 
    geotiff_paths = rasterizer.tiles.get_filenames_from_dir(base_dir = 'geotiff_remote_all_zs')
    #geotiff_paths = stager.tiles.get_filenames_from_dir(base_dir = 'geotiff_remote')

    #if ONLY_SMALL_TEST_RUN:
    #    geotiff_paths = geotiff_paths[:TEST_RUN_SIZE]
    
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
            # app_future = create_web_tiles.remote(batch, IWP_CONFIG) # remove this line
            app_future = create_web_tiles.remote(batch, config_new) 
            app_futures.append(app_future)

        for i in range(0, len(app_futures)): 
            ready, not_ready = ray.wait(app_futures)
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
        # todo: fix warning `python3.9/site-packages/geopandas/base.py:31: UserWarning: The indices of the two GeoSeries are different.`
        # with suppress(UserWarning): 
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            rasterizer.rasterize_vectors(staged_paths, make_parents=False)
    except Exception as e:
        print("⚠️ Failed to rasterize path: ", staged_paths, "\nWith error: ", e, "\nTraceback", traceback.print_exc())
    finally:
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
        print("⚠️ Failed to rasterize parent geotiffs with error ", e)
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
def stage_remote(filepath, config, logging_dict = None):
    """
    Step 1. 
    Parallelism at the per-shape-file level.
    """
    # deepcopy makes a realy copy, not using references. Helpful for parallelism.
    #config = deepcopy(CONFIG)

    try: 
        stager = pdgstaging.TileStager(config=config, check_footprints=False)
        ret = stager.stage(filepath)

        # with open(os.path.join(iwp_config['dir_output'], "workflow_log.txt"), "a+") as file:
        #         file.write(f"Successfully staged file:\n")
        #         file.write(f"{filepath}\n\n") # removed this because trying to get logger to work with special config to log.log rather than Kastan's file

        if 'Skipping' in str(ret):
            print(f"⚠️ Skipping {filepath}")

            # with open(os.path.join(iwp_config['dir_output'], "workflow_log.txt"), "a+") as file:
            #     file.write(f"SKIPPING FILE:\n")
            #     file.write(f"{filepath}\n\n") # removed this because trying to get logger to work with special config to log.log rather than Kastan's file

    except Exception as e:

        # with open(os.path.join(iwp_config['dir_output'], "workflow_log.txt"), "a+") as file:
        #     file.write(f"FAILURE TO STAGE FILE:\n")
        #     file.write(f"{filepath}\n")
        #     file.write(f"Error: {e}\n\n") # removed this because trying to get logger to work with special config to log.log rather than Kastan's file

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
    # Replace 'staged' -> '3d_tiles' in path
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
    Writes file workflow_log.txt in output directory. 
    '''
    filepath = pathlib.Path(CONFIG['dir_output'] + 'workflow_log.txt')
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.touch(exist_ok=True)
    logging.basicConfig(level=logging.INFO, filename= CONFIG['dir_output'] + 'workflow_log.txt', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')

@ray.remote
def rsync_raster_to_scatch(rsync_python_file='utilities/rsync_merge_raster_to_scratch.py'):
    while True:
        rsync_raster = f'python {rsync_python_file}'
        subprocess.run(shlex.split(rsync_raster))
        time.sleep(30)

if __name__ == '__main__':
    main()

