
import json
import logging
import logging.config
import os
import pathlib
import socket  # for seeing where jobs run when distributed
import sys
# from ray import workflow
import time
import traceback
import warnings
from asyncio.log import logger
from collections import Counter
from contextlib import suppress
from copy import deepcopy
from datetime import datetime

import pdgraster
import pdgstaging  # For staging
import ray
import viz_3dtiles  # import Cesium3DTile, Cesium3DTileset

#######################
#### Change me 😁  ####
#######################
RAY_ADDRESS       = 'ray://172.28.22.156:10001'  # SET ME!! Ray head-node IP address (using port 10001). Use output from `$ ray start --head --port=6379 --dashboard-port=8265`
# RAY_ADDRESS       = 'auto'  # SET ME!! Ray head-node IP address (using port 10001). Use output from `$ ray start --head --port=6379 --dashboard-port=8265`
NUM_PARALLEL_CPU_CORES = 192

# ALWAYS include the tailing slash "/"
# BASE_DIR_OF_INPUT = '/tmp/output/'   # The output data of MAPLE. Which is the input data for STAGING.
# BASE_DIR_OF_INPUT = '/tmp/output/'   # The output data of MAPLE. Which is the input data for STAGING.
BASE_DIR_OF_INPUT = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/'   # The output data of MAPLE. Which is the input data for STAGING.
# BASE_DIR_OF_INPUT = '/ime/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/aug_28_alaska_high_only/'   # The output data of MAPLE. Which is the input data for STAGING.
# FOOTPRINTS_PATH   = BASE_DIR_OF_INPUT + 'footprints/staged_footprints/'
FOOTPRINTS_PATH = '/tmp/staged_footprints/'

# OUTPUT            = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/aug_28_alaska_high_only/viz_output/'       # Dir for results. High I/O is good.
OUTPUT            = '/tmp/v3_viz_output/'       # Dir for results. High I/O is good.
# OUTPUT            = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/viz_output/'
# OUTPUT            = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_13_FULL_SINGLE_RUN/gpub052/july_13_fifteennode/'       # Dir for results.
# OUTPUT            = '/ime/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_24_v2/'       # Dir for results.
# OUTPUT_OF_STAGING = OUTPUT + 'staged/'              # Output dirs for each sub-step
OUTPUT_OF_STAGING = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/v3_viz_output/merged_with_base_gpub011/' + 'staged/'
# GEOTIFF_PATH      = OUTPUT + 'geotiff/'
GEOTIFF_PATH      = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/v3_viz_output/merged_geotiff_sep9/'
WEBTILE_PATH      = OUTPUT + 'web_tiles/'
THREE_D_PATH      = OUTPUT + '3d_tiles/'

# Convenience for little test runs. Change me 😁  
ONLY_SMALL_TEST_RUN = False                          # For testing, this ensures only a small handful of files are processed.
TEST_RUN_SIZE       = 10                              # Number of files to pre processed during testing (only effects testing)
##############################
#### END OF Change me 😁  ####
##############################

##################################
#### ⛔️ don't change these ⛔️ ####
##################################
IWP_CONFIG = {"dir_input": BASE_DIR_OF_INPUT,"ext_input": ".shp","dir_footprints": FOOTPRINTS_PATH,"dir_geotiff": GEOTIFF_PATH,"dir_web_tiles": WEBTILE_PATH,"dir_staged": OUTPUT_OF_STAGING,"filename_staging_summary": OUTPUT_OF_STAGING + "staging_summary.csv","filename_rasterization_events": GEOTIFF_PATH + "raster_events.csv","filename_rasters_summary": GEOTIFF_PATH + "raster_summary.csv","version": datetime.now().strftime("%B%d,%Y"),"simplify_tolerance": 0.1,"tms_id": "WorldCRS84Quad","z_range": [0, 15],"geometricError": 57,"z_coord": 0,"statistics": [    {        "name": "iwp_count",        "weight_by": "count",        "property": "centroids_per_pixel",        "aggregation_method": "sum",        "resampling_method": "sum",        "val_range": [0, None],        "palette": ["#66339952", "#d93fce", "#ffcc00"],        "nodata_val": 0,        "nodata_color": "#ffffff00"    },    {        "name": "iwp_coverage",        "weight_by": "area",        "property": "area_per_pixel_area",        "aggregation_method": "sum",        "resampling_method": "average",        "val_range": [0, 1],        "palette": ["#66339952", "#ffcc00"],        "nodata_val": 0,        "nodata_color": "#ffffff00"    },],"deduplicate_at": ["raster", "3dtiles"],"deduplicate_keep_rules": [["Date", "larger"]],"deduplicate_method": "footprints",}

def main():
    # ray.shutdown()
    # assert ray.is_initialized() == False
    print("Connecting to Ray...")
    ray.init(address=RAY_ADDRESS, dashboard_port=8265)   # most reliable way to start Ray
    # use port-forwarding to see dashboard: `ssh -L 8265:localhost:8265 kastanday@kingfisher.ncsa.illinois.edu`
    # ray.init(address='auto')                                    # multinode, but less reliable than above.
    # ray.init()                                                  # single-node only!
    assert ray.is_initialized() == True
    print("🎯 Ray initialized.")
    print(f"Writiall all results to output dir: {OUTPUT}")
    print_cluster_stats()
    
    start_logging()

    start = time.time()
    
    try:
        ########## MAIN STEPS ##########
        print("Starting main...")
        
        # (optionally) Comment out steps you don't need 😁
        step0_staging()                                     # Good staging. Simple & reliable. 
        step1_3d_tiles()                                    # Create 3D tiles from .shp
        step2_raster_highest(batch_size=300)                # rasterize highest Z level only 
        step3_raster_lower(batch_size_geotiffs=100)         # rasterize all LOWER Z levels
        step4_webtiles(batch_size_web_tiles=250)            # convert to web tiles.        

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
    
    # OLD METHOD "glob" all files. 
    # stager.tiles.add_base_dir('base_input', BASE_DIR_OF_INPUT, '.shp')
    # staging_input_files_list = stager.tiles.get_filenames_from_dir(base_dir = 'base_input')
    
    # Input files! Now we use a list of files ('iwp-file-list.json')
    try:
        staging_input_files_list_raw = json.load(open('./iwp-file-list.json'))
    except FileNotFoundError as e:
        print("Hey you, please specify a json file containing a list of input file paths (relative to `BASE_DIR_OF_INPUT`).", e)
    
    if ONLY_SMALL_TEST_RUN: # for testing only
        staging_input_files_list_raw = staging_input_files_list_raw[:TEST_RUN_SIZE]
    
    # make paths absolute 
    staging_input_files_list = prepend(staging_input_files_list_raw, BASE_DIR_OF_INPUT)
    
    # todo: automate checkpointing so it's more usable. Currently it's all manual.
    # staging_input_files_list = load_staging_checkpoints(staging_input_files_list)
    
    # catch kill signal to shutdown on command (ctrl + c)
    try: 
        # batch_size = 5
        # staged_batches = make_batch(staging_input_files_list, batch_size)
        
        # START all jobs
        print(f"\n\n👉 Staging {len(staging_input_files_list)} files in parallel 👈\n\n")
        for filepath in staging_input_files_list:    
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
                print(f"📌 Completed {i+1} of {len(staging_input_files_list)}, {(i+1)/len(staging_input_files_list)*100:.2f}%, ⏰ Elapsed time: {(time.time() - start)/60:.2f} min\n. 🔮 Estimated time to completion: { ((time.time() - start)/60) / ((i+1)/len(staging_input_files_list)) :.2f} minutes.\n")
                IP_ADDRESSES_OF_WORK.append(ray.get(ready)[0][1])

            app_futures = not_ready
            if not app_futures:
                break
    except Exception as e:
        print(f"Cauth error in Staging (step_0): {str(e)}")
    finally:
        # print FINAL stats about Staging 
        print(f"📌 Completed {i+1} of {len(staging_input_files_list)}")
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
        

def load_staging_checkpoints(staging_input_files_list):
    '''
    use CHECKPOINTS -- don't rerun already-processed-input files
    '''
    print("Using checkpoints. Skipping files that are already processed...")
    checkpoint_paths = ['/u/kastanday/success_paths.txt',
    ]
                        # '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_10_v0/success_paths.txt',
                        # '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_12_v0_tmp_resumable/success_paths.txt',
                        # '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_12_v0_tmp_resumable/run_2/success_paths.txt',
                        # '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_12_v0_tmp_resumable/run_3/success_paths.txt',]

    # combine all checkpoints
    already_processed_files_list = []
    for checkpoint_path in checkpoint_paths:
        print(f"Checkpoint path: {checkpoint_path}")
        already_processed_files_list_raw = None
        with open(checkpoint_path, 'r') as f:
            already_processed_files_list_raw = f.readlines()

        # append to list of input files to skip (already processed)
        # for filepath in already_processed_files_list_raw: 
        #     already_processed_files_list.append(filepath.strip("\n"))
            
        already_processed_files_list.extend(filepath.strip("\n") for filepath in already_processed_files_list_raw)

    # remove already processed files
    print(f"Before checkpoint total files = {len(staging_input_files_list)}. \nTotal length of already processed files: {len(already_processed_files_list)}")
    
    # return input file list minus checkpoint files
    return [i for i in staging_input_files_list if i not in already_processed_files_list]

def prep_only_high_ice_input_list():
    try:
        staging_input_files_list_raw = json.load(open('./iwp-file-list.json'))
    except FileNotFoundError as e:
        print("❌❌❌ Hey you, please specify a 👉 json file containing a list of input file paths 👈 (relative to `BASE_DIR_OF_INPUT`).", e)

    if ONLY_SMALL_TEST_RUN: # for testing only
        staging_input_files_list_raw = staging_input_files_list_raw[:TEST_RUN_SIZE]

    # make paths absolute (not relative) 
    staging_input_files_list = prepend(staging_input_files_list_raw, BASE_DIR_OF_INPUT)
    
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
def step1_3d_tiles(batch_size=300):
    
    # instantiate classes for their helper functions
    # rasterizer = pdgraster.RasterTiler(IWP_CONFIG)
    stager = pdgstaging.TileStager(IWP_CONFIG, check_footprints=False)
    
    IP_ADDRESSES_OF_WORK_3D_TILES = []
    FAILURES_3D_TILES = []
    
    print("1️⃣  Step 1 3D Tiles from Geotifs.")

    try:
        # collect staged files
        stager.tiles.add_base_dir('staging', OUTPUT_OF_STAGING, '.gpkg')
        staged_files_list = stager.tiles.get_filenames_from_dir(base_dir = 'staging')
        staged_files_list.reverse()
        
        staged_batches = make_batch(staged_files_list, batch_size)

        if ONLY_SMALL_TEST_RUN:
            staged_files_list = staged_files_list[:TEST_RUN_SIZE]
        print("total staged files len:", len(staged_files_list))

        # START JOBS
        start = time.time()
        app_futures = []
        for itr, filepath_batch in enumerate(staged_batches):
            # check if file is already done
            # filename, save_to_dir = build_filepath(filepath)
            
            # check_if_exists = False
            # if check_if_exists:
            #     filename, save_to_dir = build_step1_3d_filepath(OUTPUT, filepath)
            #     absolute_path = os.path.join(save_to_dir, filename) + ".json"
            #     if os.path.exists(absolute_path):
            #         print(f"Output already exists, skipping: {absolute_path}")
            #         continue
            #     else:
            #         print(f"Starting job ({itr}/{len(staged_files_list)}): {absolute_path}")
            #         app_futures.append(three_d_tile.remote(filepath, filename, save_to_dir))
            
            # batched version        
            app_futures.append(three_d_tile_batches.remote(filepath_batch, OUTPUT))

        # get 3D tiles, send to Tileset
        for i in range(0,len(staged_files_list)): 
            ready, not_ready = ray.wait(app_futures)
            
            # Check for failures
            if any(err in ray.get(ready)[0] for err in ["FAILED", "Failed", "❌"]):
                # failure case
                FAILURES_3D_TILES.append( [ray.get(ready)[0], ray.get(ready)[0][1]] )
                print(f"❌ Failed {ray.get(ready)[0]}")
            else:
                # success case
                print(f"✅ Finished {ray.get(ready)[0]}")
                print(f"📌 Completed {i+1} of {len(staged_batches)}, {(i+1)/len(staged_batches)*100:.2f}%, ⏰ Elapsed time: {(time.time() - start)/60:.2f} min\n")
                IP_ADDRESSES_OF_WORK_3D_TILES.append(ray.get(ready)[0])

            app_futures = not_ready
            if not app_futures:
                break
            
    except Exception as e:
        print("❌❌  Failed in main Ray loop (of Viz-3D). Error:", e)
    finally:
        print(f"⏰ Running total of elapsed time: {(time.time() - start)/60:.2f} minutes\n")
        print("Which nodes were used?")
        for ip_address, num_tasks in Counter(IP_ADDRESSES_OF_WORK_3D_TILES).items():
            print('    {} tasks on {}'.format(num_tasks, ip_address))
        print(f"There were {len(FAILURES_3D_TILES)} failures.")
        # todo -- group failures by node IP address...
        for failure_text, ip_address in FAILURES_3D_TILES:
            print(f"    {failure_text} on {ip_address}")

# @workflow.step(name="Step2_Rasterize_only_higest_z_level")
def step2_raster_highest(batch_size=100):
    """
    TODO: There is a memory leak. Something is not being deleted. This will eventually crash the Ray cluster.
    
    This is a BLOCKING step (but it can run in parallel).
    Rasterize all staged tiles (only highest z-level).
    """
    import gc
    from random import randrange

    from pympler import muppy, summary, tracker
    
    os.makedirs(GEOTIFF_PATH, exist_ok=True)

    print("2️⃣  Step 2 Rasterize only highest Z")
    stager = pdgstaging.TileStager(IWP_CONFIG, check_footprints=False)
    stager.tiles.add_base_dir('output_of_staging', OUTPUT_OF_STAGING, '.gpkg')
    
    print(f"Collecting all STAGED files from `{OUTPUT_OF_STAGING}`...")
    # Get paths to all the newly staged tiles
    staged_paths = stager.tiles.get_filenames_from_dir( base_dir = 'output_of_staging' )
    
    # stager.kas_check_footprints(staged_paths)
    
    # Add footprints path! 
    # todo: not sure this is the right extension.
    # stager.tiles.add_base_dir('input', FOOTPRINTS_PATH, '.shp')

    if ONLY_SMALL_TEST_RUN:
        staged_paths = staged_paths[:TEST_RUN_SIZE]
    
    # save a copy of the files we're rasterizing.
    staged_path_json_filepath = os.path.join(OUTPUT, "staged_paths_to_rasterize_higest.json")
    print(f"Writing a copy of the files we're rasterizing to {staged_path_json_filepath}...")
    with open(staged_path_json_filepath, "w") as f:
        json.dump(staged_paths, f, indent=2)

    print(f"Step 2️⃣ -- Making batches of staged files... batch_size: {batch_size}")
    staged_batches = make_batch(staged_paths, batch_size)

    print(f"The input to this step, Rasterization, is the output of Staging.\n Using Staging path: {OUTPUT_OF_STAGING}")

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
    print("total raster_highest: ", NUM_PARALLEL_CPU_CORES, "<-- expect this many CPUs utilized\n\n")
    logging.info(f"total STAGED filepaths (for raster_highest()): {len(staged_paths)}.")
    logging.info(f"total STAGED filepath batches (for raster_highest()): {len(staged_batches)} <-- total number of jobs to submit")
    logging.info(f"total num_parallel_cpu_cores(for raster_highest()): {NUM_PARALLEL_CPU_CORES} <-- expect this many CPUs utilized")
    
    start = time.time()
    
    # catch kill signal to shutdown on command (ctrl + c)
    try: 
        # Start remote functions
        app_futures = []
        for i, batch in enumerate(staged_batches):            
            
            # skip already done work.
            already_seen_completed_batches = 0
            if i <= already_seen_completed_batches:
                print("🚩🚩🚩🚩🚩 SKIPPING ALREADY COMPLETED WORK!")
                # continue
            else:
                app_future = rasterize.remote(batch, IWP_CONFIG)
                app_futures.append(app_future)
            
            

        for i in range(0, len(app_futures)): 
            ready, not_ready = ray.wait(app_futures)
            # print(f"✅ Finished {ray.get(ready)}")
            print(f"📌 Completed {i+1} of {len(staged_batches)}")
            print(f"⏰ Running total of elapsed time: {(time.time() - start)/60:.2f} minutes\n")

            # todo: memory debugging
            # tr.print_diff()
            
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
    stager = pdgstaging.TileStager(IWP_CONFIG, check_footprints=False)
    
    # find all Z levels
    min_z = stager.config.get_min_z()
    max_z = stager.config.get_max_z()
    parent_zs = range(max_z - 1, min_z - 1, -1)
    
    print(f"Collecting all Geotiffs (.tif) in: {GEOTIFF_PATH}")
    stager.tiles.add_base_dir('geotiff_path', GEOTIFF_PATH, '.tif') # already added ??
    
    # ADD PLACEMENT GROUP FOR ADDED STABILITY WITH MANY NODES
    print("Creating placement group for Step 2 -- raster highest")    
    from ray.util.placement_group import placement_group
    num_cpu_per_actor = 1
    pg = placement_group([{"CPU": num_cpu_per_actor}] * NUM_PARALLEL_CPU_CORES, strategy="SPREAD") # strategy="STRICT_SPREAD" strict = only one job per node. Bad. 
    ray.get(pg.ready()) # wait for it to be ready

    start = time.time()
    # Can't start lower z-level until higher z-level is complete.
    for z in parent_zs:
        print(f"👉 Starting Z level {z} of {len(parent_zs)}")
        # Loop thru Z levels 
        # Make lower z-levels based on the path names of the files just created

        if ONLY_SMALL_TEST_RUN:
            child_paths = child_paths[:TEST_RUN_SIZE]

        child_paths = stager.tiles.get_filenames_from_dir( base_dir = 'geotiff_path', z=z + 1)
        parent_tiles = set()
        for child_path in child_paths:
            parent_tile = stager.tiles.get_parent_tile(child_path)
            parent_tiles.add(parent_tile)
        parent_tiles = list(parent_tiles)

        # Break all parent tiles at level z into batches
        parent_tile_batches = make_batch(parent_tiles, batch_size_geotiffs)
        print(f"📦 Rasterizing {len(parent_tiles)} parents into {len(child_paths)} children tifs (using {len(parent_tile_batches)} batches)")
        # print(f"📦 Rasterizing {parent_tile_batches} parents")

        # PARALLELIZE batches WITHIN one zoom level (best we can do for now).
        try:
            app_futures = []
            for parent_tile_batch in parent_tile_batches:
                # MANDATORY: include placement_group for better stability on 200+ cpus.
                app_future = create_composite_geotiffs.options(placement_group=pg).remote(
                    parent_tile_batch, IWP_CONFIG, logging_dict=None)
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
    rasterizer = pdgraster.RasterTiler(IWP_CONFIG)
    stager = pdgstaging.TileStager(IWP_CONFIG, check_footprints=False)
    
    start = time.time()
    # Update color ranges
    rasterizer.update_ranges()

    print(f"Collecting all Geotiffs (.tif) in: {GEOTIFF_PATH}...")
    stager.tiles.add_base_dir('geotiff_path', GEOTIFF_PATH, '.tif')
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
        # todo: fix warning `python3.9/site-packages/geopandas/base.py:31: UserWarning: The indices of the two GeoSeries are different.`
        # with suppress(UserWarning): 
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            rasterizer.rasterize_vectors(staged_paths, make_parents=False)
    except Exception as e:
        print("⚠️ Failed to rasterize path: ", staged_paths, "\nWith error: ", e, "\nTraceback", traceback.print_exc())
    finally:
        # MEMORY LEAK (related to loggers being created)
        # for handler in logger.handlers:
        #     handler.close()
        del rasterizer
        
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

# @ray.remote(num_cpus=0.5, memory=5_000_000_000, scheduling_strategy="SPREAD")
@ray.remote
def stage_remote(filepath):
    """
    Step 1. 
    Parallelism at the per-shape-file level.
    """
    # deepcopy makes a realy copy, not using references. Helpful for parallelism.
    # config_path = deepcopy(IWP_CONFIG)
    
    try: 
        # todo: don't update `dir_input`. Only change config_path.
        # don't check for footprints in staging, only raster! 
        stager = pdgstaging.TileStager(config=IWP_CONFIG, check_footprints=False)
        stager.stage(filepath)
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
        input_file = '/home/kastanday/output/staged/WorldCRS84Quad/13/1047/104699.gpkg'
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

    try:
        # Check if the output exists. If so, skip. Do this in outer loop (less ray tasks to make)
        # absolute_path = save_to + filename + ".json"
        
        for filepath in file_batch:
            
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
            
    except Exception as e:
        return [f"‼️ ‼️ ❌ ❌ ❌ ❌ -- THIS TASK FAILED ❌ ❌ ❌ ❌ ‼️ ‼️ with path (some part of this list): {file_batch} and \nError: {e}\nTraceback: {traceback.print_exc()}", 
    socket.gethostbyname(socket.gethostname())]
    
    # success case
    return [f"Path {file_batch}", socket.gethostbyname(socket.gethostname())]

def start_logging():
    '''
    In output directory. 
    '''
    filepath = pathlib.Path(OUTPUT + 'workflow_log.txt')
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.touch(exist_ok=True)
    logging.basicConfig(level=logging.INFO, filename= OUTPUT + 'workflow_log.txt', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')
    logging.debug(f'Started ray at {RAY_ADDRESS}')



if __name__ == '__main__':
    main()

