"""
This class used to support the Staging step of the workflow. 
But it turns out it's better to just use regular ray.remote tasks (not actors) instead. 
This is an interesting method, but SUPER over-complicated for what we need.
"""


def step0_staging_actor_placement_group(shuffle_input_filepaths=False):
    # Input files! Now we use a list of files ('iwp-file-list.json')
    print("Step 0️⃣  -- Staging with SubmitActor Placement Group")

    try:
        staging_input_files_list_raw = json.load(open("./iwp-file-list.json"))
    except FileNotFoundError as e:
        print(
            "❌❌❌ Hey you, please specify a 👉 json file containing a list of input file paths 👈 (relative to `BASE_DIR_OF_INPUT`).",
            e,
        )

    if ONLY_SMALL_TEST_RUN:  # for testing only
        staging_input_files_list_raw = staging_input_files_list_raw[:TEST_RUN_SIZE]

    # make paths absolute (not relative)
    staging_input_files_list = prepend(staging_input_files_list_raw, BASE_DIR_OF_INPUT)

    # Use checkpoint! Skip already completed input files.
    # todo: uncomment for future runs.
    # print("⚠️ SKIPPING CHECKPOINT USAGE!!!!!  ⚠️")
    staging_input_files_list = load_staging_checkpoints(staging_input_files_list)

    # ONLY use high_ice for a test run.
    # print("⚠️ WARNING: ONLY USING HIGH_ICE FOR A TEST RUN!!!!!  ⚠️")
    # print("Length before filter: ", len(staging_input_files_list))
    # staging_input_files_list = list( filter(lambda filepath_str: any(keep_str in filepath_str for keep_str in ['high_ice/alaska', 'water_clipped/alaska/']), staging_input_files_list) )
    # print("Length after filter: ", len(staging_input_files_list))

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

    """
    SAVE RECORD OF CONFIG AND INPUT FILES to output dir.
    """
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


@ray.remote
class SubmitActor:
    """
    A new strategy of using Actors and placement groups.

    Create one universal queue of batches of tasks. Pass a reference to each actor (to store as a class variable).
    Then each actor just pulls from the queue until it's empty.
    """

    def __init__(self, work_queue, pg, start_time, logging_dict=None):
        self.pg = pg
        self.work_queue = work_queue  # work_queue.get() == filepath_batch
        self.num_total_jobs = self.work_queue.size()
        self.start_time = start_time
        # Distributed logging (super standard tho)
        logging.basicConfig(level=logging.INFO)

    def traverse(self, o, tree_types=(list, tuple)):
        """
        Return a flattend list of values from any nested python list or tuple.
        """
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
            logging.info(
                f"📌 Completed {num_jobs_complete} of {self.num_total_jobs}, {(num_jobs_complete)/self.num_total_jobs*100:.2f}%, ⏰ Elapsed time: {(time.time() - self.start_time)/60:.2f} min\n"
            )
            logging.info(
                f"Actor fetching new batch of jobs... Remaining jobs: {remaining_jobs}"
            )

            ##########################################################
            ##################### SCHEDULE TASKS #####################
            ##########################################################
            app_futures = []
            for filepath in self.filepath_batch:
                # filepath = /path/to/file1.shp
                app_futures.append(
                    stage_remote.options(placement_group=self.pg).remote(filepath)
                )

            ##########################################################
            ##################### GET REULTS #########################
            ##########################################################

            for _ in range(0, len(app_futures)):
                # ray.wait(app_futures) catches ONE at a time.
                ready, not_ready = ray.wait(
                    app_futures
                )  # todo , fetch_local=False do not download object from remote nodes ??
                data = ray.get(ready)
                all_returned_values = list(
                    self.traverse(data)
                )  # flatten/parse all return values.

                if any(
                    err in all_returned_values for err in ["FAILED", "Failed", "❌"]
                ):
                    # ❌ FAILURE CASE
                    # Logging is crucial for restarting jobs.
                    logging.warning(f"Failures (in this actor) = {all_returned_values}")
                    logging.warning(
                        f"❌ failed_staging_input_path: {all_returned_values[0]}"
                    )
                    logging.info(f"Num_jobs_complete = {num_jobs_complete}")
                else:
                    # ✅ SUCCESS CASE
                    # Logging is crucial for restarting jobs!
                    logging.info(
                        f"✅ successful_staging_input_path: {all_returned_values[0]}"
                    )

                app_futures = not_ready
                if not app_futures:
                    break
        return [FAILURES, IP_ADDRESSES_OF_WORK]  # not used currently


def start_actors_staging(staging_input_files_list):
    """
    1. put all filepaths-to-be-staged in queue.
    2. pass a reference to the queue to each actor.
    3. Actors just pull next thing from queue till it's empty. Then they all gracefully exit.
        ^ this happens in the actor class.
    4. Collect summary results from each actor class
    """
    from ray.util.placement_group import placement_group

    print("\n\n👉 Starting Staging with Placement Group Actors (step_0) 👈\n\n")

    batch_size = 2
    print(f"Using batchsize {batch_size}, constructing filepath batches... ")
    filepath_batches = make_batch(staging_input_files_list, batch_size=batch_size)

    num_cpu_per_actor = 1
    global NUM_PARALLEL_CPU_CORES
    max_open_files = (batch_size * NUM_PARALLEL_CPU_CORES) + NUM_PARALLEL_CPU_CORES
    NUM_PARALLEL_CPU_CORES = min(
        NUM_PARALLEL_CPU_CORES, len(filepath_batches)
    )  # if few files, use few actors.

    logging.info(
        f"NUM_PARALLEL_CPU_CORES = {NUM_PARALLEL_CPU_CORES}, batch_size = {batch_size}, max_open_files = {max_open_files}"
    )
    print(
        f"NUM_PARALLEL_CPU_CORES = {NUM_PARALLEL_CPU_CORES}, batch_size = {batch_size}, max_open_files = {max_open_files}"
    )

    print(
        "total filepath batches: ",
        len(filepath_batches),
        "<-- total number of jobs to submit",
    )
    print(
        "total submit_actors: ",
        NUM_PARALLEL_CPU_CORES,
        "<-- expect this many CPUs utilized\n\n",
    )
    logging.info(
        f"total filepath batches: {len(filepath_batches)} <-- total number of jobs to submit"
    )
    logging.info(
        f"total submit_actors: {NUM_PARALLEL_CPU_CORES} <-- expect this many CPUs utilized"
    )

    print(
        f"Creating placement group..."
    )  # todo, check that "CPU cores in total" > NUM_PARALLEL_CPU_CORES

    pg = placement_group(
        [{"CPU": num_cpu_per_actor}] * NUM_PARALLEL_CPU_CORES, strategy="SPREAD"
    )  # strategy="STRICT_SPREAD" strict = only one job per node. Bad.
    ray.get(pg.ready())  # wait for it to be ready

    # 1. put files in queue.
    # todo: I DON"T THINK QUEUE INFLUENCES NUM FILES OPEN!! These are just filepaths..... need to restrict num actors existing...
    # todo: create a dynamic limit of the number of open files.
    # todo: batch_size * NUM_PARALLEL_CPU_CORES should be < max_open_files
    from ray.util.queue import Queue

    work_queue = Queue()
    batch_iteration = 0
    for filepath_batch in filepath_batches:
        work_queue.put(filepath_batch)

        # if work_queue.size() < NUM_PARALLEL_CPU_CORES + 1:
        #     work_queue.put(filepath_batch)
        #     batch_iteration += 1

    logging.info(f"VERY INITIAL work_queue.size(): {str(work_queue.size())}")
    # 2. create actors ----- KEY IDEA HERE.
    # Create a bunch of Actor class instances. I'm wrapping my ray tasks in an Actor class.
    # For some reason, the devs showed me this yields much better performance than using Ray Tasks directly, as I was doing previously.

    start_time = time.time()  # start out here so workers have exact same start time.

    # slow start of actors!
    print("Beginning slow start of actors (every 0.25 sec)...")
    submit_actors = []
    app_futures = []
    for _ in range(NUM_PARALLEL_CPU_CORES):
        actor = SubmitActor.options(placement_group=pg).remote(
            work_queue, pg, start_time
        )
        submit_actors.append(actor)
        app_futures.append(actor.submit_jobs.remote())
        time.sleep(
            0.02
        )  # help raylet timeout error on start && reduces initial spike of network traffic.

    # collect results from SubmitActors
    for _ in range(len(app_futures)):
        try:
            ready, not_ready = ray.wait(app_futures)

            print(
                f"SubmitActor is done (below numbers should sum to: {NUM_PARALLEL_CPU_CORES})."
            )
            print("Num ready: ", len(ready))
            print("Num not_ready: ", len(not_ready))

            # add to work queue!
            # while work_queue.size() < (NUM_PARALLEL_CPU_CORES + 1):
            #     work_queue.put(filepath_batches[batch_iteration])
            #     # todo if we have more batches...
            #     batch_iteration += 1

            # this only prints after workers die, and they return here.
            logging.info(
                f"work_queue.size() -- (if it's zero, we're just finishing existing tasks.): {str(work_queue.size())}"
            )
            print(
                f"work_queue.size(): -- (if it's zero, we're just finishing existing tasks.) {str(work_queue.size())}"
            )

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
