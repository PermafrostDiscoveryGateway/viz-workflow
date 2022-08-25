
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
from asyncio.log import logger
from collections import Counter
from copy import deepcopy
from datetime import datetime

import pdgraster
import pdgstaging  # For staging
import ray
import viz_3dtiles  # import Cesium3DTile, Cesium3DTileset

#######################
#### Change me 😁  ####
#######################
RAY_ADDRESS       = 'ray://172.28.23.106:10001'  # SET ME!! Ray head-node IP address (using port 10001). Use output from `$ ray start --head --port=6379 --dashboard-port=8265`
NUM_PARALLEL_CPU_CORES = 920 # 220 # 220/960 = 23% 220

# ALWAYS include the tailing slash "/"
BASE_DIR_OF_INPUT = '/ime/bbki/kastanday/maple_data_xsede_bridges2/outputs/'   # The output data of MAPLE. Which is the input data for STAGING.
FOOTPRINTS_PATH   = BASE_DIR_OF_INPUT + 'footprints/staged_footprints/'

OUTPUT            = '/tmp/viz_output/july_30_v2/'       # Dir for results. High I/O is good.
# OUTPUT            = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_13_FULL_SINGLE_RUN/gpub052/july_13_fifteennode/'       # Dir for results.
# OUTPUT            = '/ime/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_24_v2/'       # Dir for results.
OUTPUT_OF_STAGING = OUTPUT + 'staged/'              # Output dirs for each sub-step
GEOTIFF_PATH      = OUTPUT + 'geotiff/'
WEBTILE_PATH      = OUTPUT + 'web_tiles/'
THREE_D_PATH      = OUTPUT + '3d_tiles/'

# Convenience for little test runs. Change me 😁  
ONLY_SMALL_TEST_RUN = False                          # For testing, this ensures only a small handful of files are processed.
TEST_RUN_SIZE       = 10                              # Number of files to pre processed during testing (only effects testing)
##############################
#### END OF Change me 😁  ####
##############################

##################################
#### ⛔️ don't change these ⛔️  ####
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
    
    # Logging
    # todo: auto create log file
    start_logging()

    # instantiate classes for their helper functions
    # rasterizer = pdgraster.RasterTiler(IWP_CONFIG)
    # stager = pdgstaging.TileStager(IWP_CONFIG)
    # tile_manager = stager.tiles
    # config_manager = stager.config
    start = time.time()
    
    # (optionally) CHANGE ME, if you only want certain steps 😁
    try:
        ########## MAIN STEPS ##########
        print("Starting main...")
        
        # step0_result = step0_staging()           # DEPRICATED Staging (using ray.task)
        step0_staging_actor_placement_group() #### <-------------- ⭐️ GOOD STAGING (ray.actor) ⭐️ 
        # step1_3d_tiles(stager)                         # Create 3D tiles from .shp
        # step2_raster_highest()                   # rasterize highest Z level only 
        # step3_raster_lower(batch_size_geotiffs=100)         # rasterize all LOWER Z levels
        # step4_webtiles(batch_size_web_tiles=200) # convert to web tiles.        

    except Exception as e:
        print(f"Caught error in main(): {str(e)}", "\nTraceback", traceback.print_exc())
    finally:
        # cancel work. shutdown ray.
        print(f"Runtime: {(time.time() - start)/60:.2f} minutes")
        ray.shutdown()
        assert ray.is_initialized() == False

############### 👇 MAIN STEPS FUNCTIONS 👇 ###############

# @workflow.step(name="Step0_Stage_All")
def step0_staging(stager):
    from copy import deepcopy
    
    FAILURES = []
    FAILURE_PATHS = []
    IP_ADDRESSES_OF_WORK = []
    app_futures = []
    start = time.time()
    
    print("Querying size of Ray cluster...\n")
    
    # print at start of staging
    print(f'''This cluster consists of
        {len(ray.nodes())} nodes in total
        {ray.cluster_resources()['CPU']} CPU cores in total
        {ray.cluster_resources()['memory']/1e9:.2f} GB CPU memory in total''')
    if ('GPU' in str(ray.cluster_resources())):
        print(f"        {ray.cluster_resources()['GPU']} GRAPHICCSSZZ cards in total")
    
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
    # todo refactor
    # warning something going wrong here with water_clipped paths
    staging_input_files_list = []
    for filepath in staging_input_files_list_raw:
        filepath = os.path.join(BASE_DIR_OF_INPUT, filepath)
        staging_input_files_list.append(filepath)
    
    # catch kill signal to shutdown on command (ctrl + c)
    try: 
        # batch_size = 5
        # staged_batches = make_batch(staging_input_files_list, batch_size)
        
        # Create queue of 'REMOTE' FUNCTIONS
        print(f"\n\n👉 Staging {len(staging_input_files_list)} files in parallel 👈\n\n")
        for filepath in staging_input_files_list:    
            # create list of remote function ids (i.e. futures)
            app_futures.append(stage_remote.remote(filepath))

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
                print(f"📌 Completed {i+1} of {len(staging_input_files_list)}, {(i+1)/len(staging_input_files_list)*100:.1f}%, ⏰ Elapsed time: {(time.time() - start)/60:.2f} min\n")
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


@ray.remote
class SubmitActor:
    '''
    A new strategy of using Actors and placement groups.

    Create one universal queue of batches of tasks. Pass a reference to each actor (to store as a class variable).
    Then each actor just pulls from the queue until it's empty. 
    '''

    def __init__(self, work_queue, pg, start_time, logging_dict=None):
        self.pg = pg
        self.work_queue = work_queue  # work_queue.get() == filepath_batch
        self.num_total_jobs = self.work_queue.size()
        self.start_time = start_time
        # Distributed logging (super standard tho)
        logging.basicConfig(level=logging.INFO)


    def traverse(self, o, tree_types=(list, tuple)):
        '''
        Return a flattend list of values from any nested python list or tuple.
        '''
        if isinstance(o, tree_types):
            for value in o:
                for subvalue in self.traverse(value, tree_types):
                    yield subvalue
        else:
            yield o

    def submit_jobs(self):
        print("Starting new actor...")
        
        # print(f"📌 Submitting {self.work_queue.size()} jobs to {self.pg}")
        
        FAILURES = []
        IP_ADDRESSES_OF_WORK = []
        
        while not self.work_queue.empty():
            # Get work from the queue.
            self.filepath_batch = self.work_queue.get()
            remaining_jobs = self.work_queue.size()
            
            num_jobs_complete = self.num_total_jobs - remaining_jobs
            logging.info(f"📌 Completed {num_jobs_complete} of {self.num_total_jobs}, {(num_jobs_complete)/self.num_total_jobs*100:.2f}%, ⏰ Elapsed time: {(time.time() - self.start_time)/60:.2f} min\n")
            logging.info(f"Actor fetching new batch of jobs... Remaining jobs: {remaining_jobs}")
            
            ##########################################################
            ##################### SCHEDULE TASKS #####################
            ##########################################################
            app_futures = []
            for filepath in self.filepath_batch:
                # filepath = /path/to/file1.shp
                app_futures.append(stage_remote.options(placement_group=self.pg).remote(filepath))

            ##########################################################
            ##################### GET REULTS #########################
            ##########################################################
            
            for _ in range(0, len(app_futures)): 
                # ray.wait(app_futures) catches ONE at a time. 
                ready, not_ready = ray.wait(app_futures) # todo , fetch_local=False do not download object from remote nodes ??
                data = ray.get(ready)
                all_returned_values = list(self.traverse(data)) # flatten/parse all return values.
                
                if any(err in all_returned_values for err in ["FAILED", "Failed", "❌"]):
                    # ❌ FAILURE CASE
                    # Logging is crucial for restarting jobs.
                    logging.warning(f"Failures (in this actor) = {all_returned_values}")
                    logging.warning(f"❌ failed_staging_input_path: {all_returned_values[0]}")
                    logging.info(f"Num_jobs_complete = {num_jobs_complete}")
                else:
                    # ✅ SUCCESS CASE 
                    # Logging is crucial for restarting jobs!
                    logging.info(f"✅ successful_staging_input_path: {all_returned_values[0]}")
                    
                app_futures = not_ready
                if not app_futures:
                    break
        return [FAILURES, IP_ADDRESSES_OF_WORK] # not used currently

def start_actors_staging(staging_input_files_list):
    '''
    1. put all filepaths-to-be-staged in queue. 
    2. pass a reference to the queue to each actor. 
    3. Actors just pull next thing from queue till it's empty. Then they all gracefully exit. 
        ^ this happens in the actor class.
    4. Collect summary results from each actor class
    '''
    from ray.util.placement_group import placement_group
    
    print("\n\n👉 Starting Staging with Placement Group Actors (step_0) 👈\n\n")
    

    batch_size = 2
    print(f"Using batchsize {batch_size}, constructing filepath batches... ")
    filepath_batches = make_batch(staging_input_files_list, batch_size=batch_size)
    
    num_cpu_per_actor = 1
    global NUM_PARALLEL_CPU_CORES
    max_open_files = (batch_size * NUM_PARALLEL_CPU_CORES) + NUM_PARALLEL_CPU_CORES
    NUM_PARALLEL_CPU_CORES = min(NUM_PARALLEL_CPU_CORES, len(filepath_batches)) # if few files, use few actors.
    
    logging.info(f'NUM_PARALLEL_CPU_CORES = {NUM_PARALLEL_CPU_CORES}, batch_size = {batch_size}, max_open_files = {max_open_files}')
    print(f'NUM_PARALLEL_CPU_CORES = {NUM_PARALLEL_CPU_CORES}, batch_size = {batch_size}, max_open_files = {max_open_files}')
    
    print("total filepath batches: ", len(filepath_batches), "<-- total number of jobs to submit")
    print("total submit_actors: ", NUM_PARALLEL_CPU_CORES, "<-- expect this many CPUs utilized\n\n")
    logging.info(f"total filepath batches: {len(filepath_batches)} <-- total number of jobs to submit")
    logging.info(f"total submit_actors: {NUM_PARALLEL_CPU_CORES} <-- expect this many CPUs utilized")

    
    print(f"Creating placement group...") # todo, check that "CPU cores in total" > NUM_PARALLEL_CPU_CORES
    
    pg = placement_group([{"CPU": num_cpu_per_actor}] * NUM_PARALLEL_CPU_CORES, strategy="SPREAD") # strategy="STRICT_SPREAD" strict = only one job per node. Bad. 
    ray.get(pg.ready()) # wait for it to be ready

    # 1. put files in queue. 
    # todo: I DON"T THINK QUEUE INFLUENCES NUM FILES OPEN!! These are just filepaths..... need to restrict num actors existing...
    # todo: create a dynamic limit of the number of open files.
    # todo: batch_size * NUM_PARALLEL_CPU_CORES should be < max_open_files
    from ray.util.queue import Queue
    work_queue = Queue()
    batch_iteration = 0
    for filepath_batch in filepath_batches:
        work_queue.put(filepath_batch)
        # todo -- remove this.
        # if work_queue.size() < NUM_PARALLEL_CPU_CORES + 1:
        #     work_queue.put(filepath_batch)
        #     batch_iteration += 1
        
    logging.info(f"VERY INITIAL work_queue.size(): {str(work_queue.size())}")
    # 2. create actors ----- KEY IDEA HERE.
    # Create a bunch of Actor class instances. I'm wrapping my ray tasks in an Actor class. 
    # For some reason, the devs showed me this yields much better performance than using Ray Tasks directly, as I was doing previously.
    
    start_time = time.time() # start out here so workers have exact same start time.
    
    # slow start of actors!
    print("Beginning slow start of actors (every 0.25 sec)...")
    submit_actors=[]
    app_futures=[]
    for _ in range(NUM_PARALLEL_CPU_CORES):
        actor = SubmitActor.options(placement_group=pg).remote(work_queue, pg, start_time) 
        submit_actors.append(actor)
        app_futures.append(actor.submit_jobs.remote())
        time.sleep(0.02) # help raylet timeout error on start && reduces initial spike of network traffic.
    

    # collect results from SubmitActors    
    for _ in range(len(app_futures)):
        try:
            ready, not_ready = ray.wait(app_futures)
            
            print(f"SubmitActor is done (below numbers should sum to: {NUM_PARALLEL_CPU_CORES}).")
            print("Num ready: ", len(ready))
            print("Num not_ready: ", len(not_ready))
            
            # add to work queue! 
            # while work_queue.size() < (NUM_PARALLEL_CPU_CORES + 1):
            #     work_queue.put(filepath_batches[batch_iteration])
            #     # todo if we have more batches... 
            #     batch_iteration += 1
            
            # this only prints after workers die, and they return here.
            logging.info(f"work_queue.size(): (should be decreasing) {str(work_queue.size())}")
            print(f"work_queue.size(): (should be decreasing) {str(work_queue.size())}")
            
            if len(ready) + len(not_ready) != NUM_PARALLEL_CPU_CORES:
                print("❌❌❌❌ BAD!!! SOME ACTORS HAVE DIED!!")
                logging.error(f"❌❌❌❌ BAD!!! SOME ACTORS HAVE DIED!!")
            
            # break loop when all actors are dead.
            if len(ready) == 0 and len(not_ready) == 0:
                return "All SubmitActors are done or dead."
            
            if len(ray.get(ready)[0][0]) > 0:
                print("PRINTING Failures (per actor):", ray.get(ready)[0][0])
        except Exception as e:
            print(e)
    
    print("🎉 All submit actors have returned.  DONE STAGING...")
    
    return

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
    checkpoint_paths = ['/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_10_v0/success_paths.txt',
                        '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_12_v0_tmp_resumable/success_paths.txt',
                        '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_12_v0_tmp_resumable/run_2/success_paths.txt',
                        '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_12_v0_tmp_resumable/run_3/success_paths.txt',]

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

# @workflow.step(name="Step0_Stage_All")
def step0_staging_actor_placement_group(shuffle_input_filepaths=False):
    # Input files! Now we use a list of files ('iwp-file-list.json')
    print("Step 0️⃣  -- Staging with SubmitActor Placement Group")
    
    try:
        staging_input_files_list_raw = json.load(open('./iwp-file-list.json'))
    except FileNotFoundError as e:
        print("❌❌❌ Hey you, please specify a 👉 json file containing a list of input file paths 👈 (relative to `BASE_DIR_OF_INPUT`).", e)

    if ONLY_SMALL_TEST_RUN: # for testing only
        staging_input_files_list_raw = staging_input_files_list_raw[:TEST_RUN_SIZE]

    # make paths absolute (not relative) 
    staging_input_files_list = prepend(staging_input_files_list_raw, BASE_DIR_OF_INPUT)
    
    # Use checkpoint! Skip already completed input files. 
    # todo: uncomment for future runs.
    print("⚠️ SKIPPING CHECKPOINT USAGE!!!!!  ⚠️")
    # staging_input_files_list = load_staging_checkpoints(staging_input_files_list)
    
    # shuffle the list 
    # Idea: this should increase speed by reducing write conflicts wait times. 
    # Write conflicts are avoided with a file lock. Causes lots of waiting when reading files sequentially.
    if shuffle_input_filepaths:
        import random
        print("Shuffling input filepaths (to increase Staging() speed)...")
        print("Beofre shuffle:")
        print(staging_input_files_list[:3])
        random.shuffle(staging_input_files_list)
        print("After shuffle:")
        print(staging_input_files_list[:3])
    
    '''
    SAVE RECORD OF CONFIG AND INPUT FILES to output dir.
    '''
    # save len of input
    global LEN_STAGING_FILES_LIST
    LEN_STAGING_FILES_LIST = len(staging_input_files_list)
    
    # Save input file list in output dir, for reference and auditing.
    json_filepath = os.path.join(OUTPUT, "staging_input_files_list.json")
    # check if filepath exists, make if not.
    filepath = pathlib.Path(json_filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.touch(exist_ok=True)
    with open(json_filepath, "w") as f:
        json.dump(staging_input_files_list, f, indent=4, sort_keys=True)
        
    # save iwp-config to output dir
    # IWP_config_json = json.dumps(IWP_CONFIG, indent=2)
    iwp_config_json_filepath = os.path.join(OUTPUT, "IWP_config.json")
    with open(iwp_config_json_filepath, "w") as f:
        json.dump(IWP_CONFIG, f, indent=2)

    # start actors!
    start_actors_staging(staging_input_files_list)
    
    return

def prepend(mylist, mystr):
    '''
    Prepend str to front of each element of List. Typically filepaths.
    '''
    return [mystr + item for item in mylist]

# @workflow.step(name="Step1_3D_Tiles")
def step1_3d_tiles(stager):
    IP_ADDRESSES_OF_WORK_3D_TILES = []
    FAILURES_3D_TILES = []
    
    print("1️⃣  Step 2 Rasterize only highest Z")

    try:
        # collect staged files
        stager.tiles.add_base_dir('staging', OUTPUT_OF_STAGING, '.gpkg')
        staged_files_list = stager.tiles.get_filenames_from_dir(base_dir = 'staging')

        if ONLY_SMALL_TEST_RUN:
            staged_files_list = staged_files_list[:TEST_RUN_SIZE]
        print("total staged files len:", len(staged_files_list))

        # START JOBS
        start = time.time()
        app_futures = []
        for filepath in staged_files_list:
            filename, save_to_dir = build_filepath(filepath)
            app_futures.append(three_d_tile.remote(filepath, filename, save_to_dir))

        # get 3D tiles, send to Tileset
        for i in range(0,len(staged_files_list)): 
            ready, not_ready = ray.wait(app_futures)
            
            # Check for failures
            if any(err in ray.get(ready)[0][0] for err in ["FAILED", "Failed", "❌"]):
                # failure case
                FAILURES_3D_TILES.append( [ray.get(ready)[0][0], ray.get(ready)[0][1]] )
                print(f"❌ Failed {ray.get(ready)[0][0]}")
            else:
                # success case
                print(f"✅ Finished {ray.get(ready)[0][0]}")
                print(f"📌 Completed {i+1} of {len(staged_files_list)}, {(i+1)/len(staged_files_list)*100}%, ⏰ Elapsed time: {(time.time() - start)/60:.2f} min\n")
                IP_ADDRESSES_OF_WORK_3D_TILES.append(ray.get(ready)[0][1])

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
    This is a BLOCKING step (but it can run in parallel).
    Rasterize all staged tiles (only highest z-level).
    """
    print("2️⃣  Step 2 Rasterize only highest Z")
    stager = pdgstaging.TileStager(IWP_CONFIG)
    
    print("Collecting all staged files...")
    # Get paths to all the newly staged tiles
    stager.tiles.add_base_dir('output_of_staging', OUTPUT_OF_STAGING, '.gpkg')
    staged_paths = stager.tiles.get_filenames_from_dir( base_dir = 'output_of_staging' )
    
    # Add footprints path! 
    # todo: not sure this is the right extension.
    # stager.tiles.add_base_dir('input', FOOTPRINTS_PATH, '.shp')

    if ONLY_SMALL_TEST_RUN:
        staged_paths = staged_paths[:TEST_RUN_SIZE]

    print(f"Step 2️⃣ -- Making batches of staged files... batch_size: {batch_size}")
    staged_batches = make_batch(staged_paths, batch_size)

    print(f"The input to this step, Rasterization, is the output of Staging.\n Using Staging path: {OUTPUT_OF_STAGING}")

    print(f"🌄 Rasterize total files {len(staged_paths)} gpkgs, using batch size: {batch_size}")
    print(f"🏎  Parallel batches of jobs: {len(staged_batches)}...\n")

    
    # ADD PLACEMENT GROUP FOR ADDED STABILITY WITH MANY NODES
    print("Creating placement group for Step 2 -- raster highest...")    
    from ray.util.placement_group import placement_group
    num_cpu_per_actor = 1
    pg = placement_group([{"CPU": num_cpu_per_actor}] * NUM_PARALLEL_CPU_CORES, strategy="SPREAD") # strategy="STRICT_SPREAD" strict = only one job per node. Bad. 
    ray.get(pg.ready()) # wait for it to be ready
    
    print(f"total filepaths: {len(staged_paths)}.")
    print("total filepath batches: ", len(staged_batches), "<-- total number of jobs to submit")
    print("total submit_actors: ", NUM_PARALLEL_CPU_CORES, "<-- expect this many CPUs utilized\n\n")
    logging.info(f"total filepaths: {len(staged_paths)}.")
    logging.info(f"total filepath batches: {len(staged_batches)} <-- total number of jobs to submit")
    logging.info(f"total submit_actors: {NUM_PARALLEL_CPU_CORES} <-- expect this many CPUs utilized")
    

    start = time.time()

    # catch kill signal to shutdown on command (ctrl + c)
    try: 
        # Start remote functions
        app_futures = []
        for batch in staged_batches:            
            # QUEUE JOBS
            # MANDATORY: include placement_group for better stability on 200+ cpus.
            app_future = rasterize.options(placement_group=pg).remote(batch, IWP_CONFIG)
            app_futures.append(app_future)

        for i in range(0, len(app_futures)): 
            ready, not_ready = ray.wait(app_futures)
            # print(f"✅ Finished {ray.get(ready)}")
            print(f"📌 Completed {i+1} of {len(staged_batches)}")
            print(f"⏰ Running total of elapsed time: {(time.time() - start)/60:.2f} minutes\n")
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
    stager = pdgstaging.TileStager(IWP_CONFIG)
    
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
        print(f"📦 Rasterizing {len(parent_tile_batches)} parents into {len(child_paths)} children tifs")
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
                print(f"📌 Completed {i+1} of {len(parent_tile_batches)}")
                print(f"📌 Completed {i+1} of {len(parent_tile_batches)}, {(i+1)/len(parent_tile_batches)*100}%")
                print(f"⏰ Running total of elapsed time: {(time.time() - start)/60:.2f} minutes\n")
                app_futures = not_ready
                if not app_futures:
                    break
        except Exception as e:
            print(f"Cauth error in Ray loop in step 3: {str(e)}")

    print(f"⏰ Total time to create parent geotiffs: {(time.time() - start)/60:.2f} minutes\n")
    return "Done step 3."

# @workflow.step(name="Step4_create_webtiles")
def step4_webtiles(batch_size_web_tiles=100):
    '''
    STEP 4: Create web tiles from geotiffs
    Infinite parallelism.
    '''
    print("4️⃣ -- Creating web tiles from geotiffs...")
    
    # instantiate classes for their helper functions
    rasterizer = pdgraster.RasterTiler(IWP_CONFIG)
    stager = pdgstaging.TileStager(IWP_CONFIG)
    
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
    print("Creating placement group for Step 2 -- raster highest")    
    from ray.util.placement_group import placement_group
    num_cpu_per_actor = 1
    pg = placement_group([{"CPU": num_cpu_per_actor}] * NUM_PARALLEL_CPU_CORES, strategy="SPREAD") # strategy="STRICT_SPREAD" strict = only one job per node. Bad. 
    ray.get(pg.ready()) # wait for it to be ready
    
    # catch kill signal to shutdown on command (ctrl + c)
    try: 
        # Start remote functions
        app_futures = []
        for batch in geotiff_batches:
            # MANDATORY: include placement_group for better stability on 200+ cpus.
            app_future = create_web_tiles.options(placement_group=pg).remote(batch, IWP_CONFIG)
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
        ray.shutdown()
        assert ray.is_initialized() == False

    # Don't record end time until all web tiles have been created
    [a.result() for a in app_futures]

    print(f'⏰ Total time to create web tiles:  {len(geotiff_paths)} tiles: {(time.time() - start)/60:.2f} minutes\n')

############### HELPER FUNCTIONS BELOW HERE ###############

@ray.remote
def rasterize(staged_paths, config, logging_dict=None):
    """
    Rasterize a batch of vector files (step 2)
    """
    config['dir_input'] = OUTPUT_OF_STAGING
    import pdgraster
    if logging_dict:
        import logging.config
        logging.config.dictConfig(logging_dict)

    try:
        rasterizer = pdgraster.RasterTiler(config)
        rasterizer.rasterize_vectors(staged_paths, make_parents=False)
    except Exception as e:
        print("⚠️ Failed to rasterize path: ", staged_paths, "\nWith error: ", e, "\nTraceback", traceback.print_exc())
        
    return staged_paths

@ray.remote
def create_composite_geotiffs(tiles, config, logging_dict=None):
    """
    Make composite geotiffs (step 3)
    """
    import pdgraster
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
    config_path = deepcopy(IWP_CONFIG)
    
    try: 
        config_path['dir_input'] = filepath
        # don't check for footprints in staging, only raster! 
        # todo refactor: ensure 'deduplicate_at' does not include 'staging' or any part of 'stage'. 
        stager = pdgstaging.TileStager(config=config_path, check_footprints=False)
        # stager.stage_all()
        stager.stage(config_path['dir_input'])
    except Exception as e:
        return [config_path['dir_input'],
                socket.gethostbyname(socket.gethostname()),
                "‼️ ‼️ ❌ ❌ ❌ ❌ -- THIS TASK FAILED ❌ ❌ ❌ ❌ ‼️ ‼️"]
        
    # return [input_path, host_computer]
    return [config_path['dir_input'], 
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

@ray.remote
def three_d_tile(input_file, filename, save_to):
    try:
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

