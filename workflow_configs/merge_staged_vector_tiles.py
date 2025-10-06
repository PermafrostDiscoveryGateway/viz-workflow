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
ray.init(
    address=f"ray://{head_ip}:10001", dashboard_port=8265
)  # most reliable way to start Ray
# use port-forwarding to see dashboard: `ssh -L 8265:localhost:8265 kastanday@kingfisher.ncsa.illinois.edu`
assert ray.is_initialized() == True
print("🎯 Ray initialized.")

import PRODUCTION_IWP_CONFIG

IWP_CONFIG = PRODUCTION_IWP_CONFIG.IWP_CONFIG
# update the config for the current context
IWP_CONFIG["dir_staged"] = IWP_CONFIG["dir_staged_remote"]
IWP_CONFIG["dir_footprints"] = IWP_CONFIG["dir_footprints_local"]


def main():
    """
    Usage Instructions:
        - Enter the HEAD node (lowest # node) at the end of the path for `merged_dir_path`
        - Enter all worker nodes (all other nodes) at the end of the paths that make
        up `staged_dir_paths_list`
    """

    #######################
    #### Change me 😁  ####
    #######################
    # todo -- get files from dirs automatically, using os.lsdir().
    # BASE_DIR = '/scratch/bbou/julietcohen/IWP/output/...'
    merged_dir_path = f"{IWP_CONFIG['dir_staged']}gpub020"  # this path SHOULD NOT be in the `staged_dir_paths_list`
    staged_dir_paths_list = [
        f"{IWP_CONFIG['dir_staged']}gpub041",
        # f"{IWP_CONFIG['dir_staged']}gpub041",
        # f"{IWP_CONFIG['dir_staged']}gpub041",
        # f"{IWP_CONFIG['dir_staged']}gpub038",
        # f"{IWP_CONFIG['dir_staged']}gpub027",
        # f"{IWP_CONFIG['dir_staged']}gpub028",
        # f"{IWP_CONFIG['dir_staged']}gpub029",
        # f"{IWP_CONFIG['dir_staged']}gpub030",
        # f"{IWP_CONFIG['dir_staged']}gpub031",
        # f"{IWP_CONFIG['dir_staged']}gpub032",
        # f"{IWP_CONFIG['dir_staged']}gpub033",
        # f"{IWP_CONFIG['dir_staged']}gpub034",
        # f"{IWP_CONFIG['dir_staged']}gpub035",
        # f"{IWP_CONFIG['dir_staged']}gpub036",
        # f"{IWP_CONFIG['dir_staged']}gpub037",
        # f"{IWP_CONFIG['dir_staged']}gpub038",
        # f"{IWP_CONFIG['dir_staged']}gpub039",
        # f"{IWP_CONFIG['dir_staged']}gpub040",
        # f"{IWP_CONFIG['dir_staged']}gpub041",
        # f"{IWP_CONFIG['dir_staged']}gpub___",
    ]
    ##############################
    #### END OF Change me 😁  ####
    ##############################

    # validate input
    for path in staged_dir_paths_list + [merged_dir_path]:
        # check that path exists
        if not os.path.exists(path):
            raise ValueError(
                f"Path {path} does not exist. For usage instructions, read the top of this file."
            )

    print("Input dirs: ", "\n".join(staged_dir_paths_list))
    print("Final  dir: ", merged_dir_path)

    # stager = pdgstaging.TileStager(config=IWP_CONFIG, check_footprints=False)
    stager = TileStager(config=IWP_CONFIG, check_footprints=False)
    ext = ".gpkg"

    print("Starting merge...")
    start_distributed_logging()
    merger = StagingMerger()
    merger.merge_all_staged_dirs(staged_dir_paths_list, merged_dir_path, stager, ext)


class StagingMerger:
    def __init__(
        self,
    ):
        self.final_merged_paths_set = None
        self.append_count = 0
        self.skipped_merge_identical_file_count = 0
        self.fast_copy_count = 0
        self.isDestructive = False  # Decide between mv and cp!

    def merge_all_staged_dirs(
        self, staged_dir_paths_list, merged_dir_path, stager, ext=None
    ):
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
        final_merged_paths, final_merged_paths_set = self.collect_paths_from_dir(
            path_manager, "merged_dir_path", merged_dir_path, ext
        )

        # MERGE! For each staged_dir (equal to number of original compute nodes)!
        for i, staged_dir_path in enumerate(staged_dir_paths_list):

            # collect paths
            paths, paths_set = self.collect_paths_from_dir(
                path_manager, f"staged_dir_path{i}", staged_dir_path, ext
            )
            incoming_length = len(paths_set)

            print(f"Merged dir paths: {len(final_merged_paths)}")
            print(
                f"Incoming staged dir paths ({i + 1} of {len(staged_dir_paths_list)}): {incoming_length}"
            )

            try:
                # Start remote functions
                app_futures = []

                path_batches = make_batch(paths, batch_size=800)
                total_batches = len(path_batches)
                for j, incoming_tile_in_path_batch in enumerate(path_batches):
                    print(f"starting batch {j+1} of {total_batches}")

                    # collect all the out paths
                    incoming_tile_out_path_batch = []
                    for incoming_tile_in_path in incoming_tile_in_path_batch:
                        incoming_tile_out_path_batch.append(
                            path_manager.path_from_tile(
                                tile=incoming_tile_in_path, base_dir="merged_dir_path"
                            )
                        )

                    # app_future = batch_merge_tile.remote(incoming_tile_in_path_batch, incoming_tile_out_path_batch, isDestructive=self.isDestructive, stager.config.input_config)
                    # batch_merge_tile(incoming_tile_in_path_batch, incoming_tile_out_path_batch, isDestructive, stager):
                    app_future = batch_merge_tile.remote(
                        incoming_tile_in_path_batch,
                        incoming_tile_out_path_batch,
                        isDestructive=self.isDestructive,
                        stager=stager.config.input_config,
                    )
                    app_futures.append(app_future)
                # for incoming_tile_in_path in paths:
                #     NOT BATCHED VERSION:
                #     incoming_tile_out_path = path_manager.path_from_tile(tile=incoming_tile_in_path, base_dir='merged_dir_path')
                #     app_future = self.merge_tile.remote(incoming_tile_in_path, incoming_tile_out_path, self.isDestructive)
                #     app_futures.append(app_future)

                # collect results
                for k in range(0, len(app_futures)):
                    ready, not_ready = ray.wait(app_futures)

                    print(
                        f"{ray.get(ready)}\n✅ Completed batch {k+1} of {total_batches}. Actions taken ^^"
                    )
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
                print(
                    f"Runtime: {(time.monotonic() - merge_all_vector_tiles_start_time):.2f} seconds"
                )

            print(
                f"⏰ Total time to merge {incoming_length} tiles: {(time.monotonic() - merge_all_vector_tiles_start_time)/60:.2f} minutes\n"
            )

        print("Done, exiting...")
        return

    def collect_paths_from_dir(
        self, path_manager, base_dir_name_string, base_dir_path, ext
    ):
        """
        Collect all the paths from a directory that match an extension.

        Parameters
        ----------
        path_manager
            `path_manager = TileStager(stager).tiles`
        base_dir_name_string
            A human-readable name to use as an alias for this path ()
        merged_dir_path

        """
        # This is where a cache of path lists (one for each compute node), will be stored.
        paths_list_local_filename = f"./path_list_cache/{base_dir_name_string}.txt"

        ## IF PATHS WERE SAVED to a file, much faster. ELSE: Collect all paths from NFS file server.
        paths_list = []
        pathlib_paths_list_local_filename = pathlib.Path(paths_list_local_filename)

        # DON'T USE A CACHE. CAUSES HARD BUGS. NO CACHE: Collect all paths from NFS file server.
        if False:  # pathlib_paths_list_local_filename.exists():
            with open(paths_list_local_filename, "r") as fp:
                for line in fp:
                    # remove linebreak from each line
                    paths_list.append(line[:-1])
            paths_set = set(paths_list)  # speed optimization
            self.final_merged_paths_set = paths_set
            assert len(paths_list) == len(
                paths_set
            ), f"❌ Warning: There are duplicate paths in this base dir: {base_dir_path}"
            path_manager.add_base_dir(base_dir_name_string, base_dir_path, ext)
        else:
            start = time.monotonic()
            print(
                f"Collecting paths. Base dir named: {base_dir_name_string}  \n\tpath: {base_dir_path}"
            )
            path_manager.add_base_dir(base_dir_name_string, base_dir_path, ext)
            paths_list = path_manager.get_filenames_from_dir(base_dir_name_string)
            paths_set = set(paths_list)  # speed optimization
            self.final_merged_paths_set = paths_set

            assert len(paths_list) == len(
                paths_set
            ), f"❌ Warning: There are duplicate paths in this base dir: {base_dir_path}"

            print(f"⏰ Elapsed time: {(time.monotonic() - start)/60:.2f} minutes.")

            # write path list to disk
            pathlib_paths_list_local_filename.parent.mkdir(parents=True, exist_ok=True)
            pathlib_paths_list_local_filename.touch(exist_ok=True)
            with open(paths_list_local_filename, "w") as outfile:
                # todo: write oritinal filepath too as first line!
                outfile.write("\n".join(str(path) for path in paths_list))

        return paths_list, paths_set


"""
###############################################
End class methods
###############################################
"""


def make_batch(items, batch_size):
    """
    Simple helper.
    Create batches of a given size from a list of items.
    """
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


@ray.remote
def batch_merge_tile(
    incoming_tile_in_path_batch, incoming_tile_out_path_batch, isDestructive, stager
):
    """
    Merge a batch of tiles. More efficient by making fewer ray remote calls.
    """
    assert len(incoming_tile_in_path_batch) == len(
        incoming_tile_out_path_batch
    ), f"❌ Error: the 'in-paths' and 'out-paths' must match. You gave me: {len(incoming_tile_in_path_batch)} != {len(incoming_tile_out_path_batch)}"
    actions_taken = []
    for incoming_tile_in_path, incoming_tile_out_path in zip(
        incoming_tile_in_path_batch, incoming_tile_out_path_batch
    ):
        action_taken_string = merge_tile(
            incoming_tile_in_path, incoming_tile_out_path, isDestructive, stager
        )
        actions_taken.append(action_taken_string)

    return actions_taken


# @ray.remote
def merge_tile(incoming_tile_in_path, incoming_tile_out_path, isDestructive, stager):
    if isinstance(stager, (dict, str)):
        stager = TileStager(stager, check_footprints=False)
    # todo check that this comparison is lining up...
    action_taken_string = ""
    ## if the head node path + subdirs to that specific staged file does not exist:
    if not os.path.exists(incoming_tile_out_path):
        # time.sleep(5)
        # (1) add to final_merged_paths_set
        # (2) copy incoming to destination

        # NO NEED: final_merged_paths_set.add(incoming_tile_out_path)

        ## then define the head node path as a path
        ## and if the head node dir that holds the file is not a dir in, make dir
        # check the destination folder structure exists
        filepath = pathlib.Path(incoming_tile_out_path)
        if not filepath.parent.is_dir():
            filepath.parent.mkdir(parents=True, exist_ok=True)

        ## default for isDestructive is False, so we copy the file with pyfastcopy rather than move it
        if isDestructive:
            shutil.move(incoming_tile_in_path, incoming_tile_out_path)
            # print("File moved.")
            # print("File not in dest. Moving to dest: ", incoming_tile_out_path)
            action_taken_string += "fast move"
        else:
            # faster with pyfastcopy
            shutil.copyfile(incoming_tile_in_path, incoming_tile_out_path)
            # print("File not in dest. Copying to dest: ", incoming_tile_out_path)
            # print("File coppied.")
            action_taken_string += "fast copy"
    ## if the head node path + subdirs to that specific staged file does exist,
    ## compare the two files and skip if they are the same,
    ## and if the files are not the same, then append the polygons to the exisitng tile
    else:
        # if identical, skip. Else, merge/append-to the GDF.
        if filecmp.cmp(incoming_tile_in_path, incoming_tile_out_path):
            # identical, skip merge
            ## default for isDestructive is False
            if isDestructive:
                os.remove(incoming_tile_in_path)
                action_taken_string += "identical. Deleted old."
            # self.skipped_merge_identical_file_count += 1
            else:
                action_taken_string += "identical. skipped"
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
                    # read in the file that is not in the head node
                    incoming_gdf = gpd.read_file(incoming_tile_in_path)
                    # why would we use combine_and_deduplicate() to just append 2 geodatframes?
                    # in the ray workflow, we do not deduplicate at staging, we do it at raster & 3dtiles
                    # so seems like we are adding complexity to this appending step
                    dedup_method = stager.config.get_deduplication_method()
                    if dedup_method is not None:
                        mode = "w"
                        ## concatenate the geodataframes and identify duplicates:
                        ## ensuring that the tiles within the head node contains
                        ## all the polygons for that tile, regardless of which node
                        ## processed each polygon
                        incoming_gdf = stager.combine_and_deduplicate(
                            incoming_gdf, incoming_tile_out_path
                        )
                    else:
                        mode = "a"
                    incoming_gdf.to_file(incoming_tile_out_path, mode=mode)
                except Exception as e:
                    # todo: implement logging w/ ray's distributed logger.
                    print("❌ Error: ", e)
                    print("❌ Error: ", incoming_tile_in_path)
                    print("❌ Error: ", incoming_tile_out_path)
                    print("one of the above files was probably corrupted.")
                    logging.error(
                        f"Error: {e} \n\tIncoming path: {incoming_tile_in_path} \n\tOutput path: {incoming_tile_out_path}"
                    )
                    # exit()

                release_file(in_path_lock)
                release_file(out_path_lock)

                # if isDestructive:
                #     # delete original file

                # always delete after two tiles are merged, otherwise it's impossible to keep things consistent.
                os.remove(incoming_tile_in_path)

            # print("appended & old deleted.")
            action_taken_string += f"Merged and old deleted."
            # self.append_count += 1
    return action_taken_string


def start_distributed_logging():
    """
    In output directory.
    """
    log_filename = make_workflow_id("merge_staged_tiles")
    filepath = pathlib.Path(IWP_CONFIG["dir_staged"] + log_filename)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.touch(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        filename=IWP_CONFIG["dir_staged"] + "workflow_log.txt",
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def make_workflow_id(name: str) -> str:
    import pytz
    from datetime import datetime

    # Timezones: US/{Pacific, Mountain, Central, Eastern}
    # All timezones `pytz.all_timezones`. Always use caution with timezones.
    curr_time = datetime.now(pytz.timezone("US/Central"))
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
    lock_path = path + ".lock"
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


if __name__ == "__main__":
    main()
