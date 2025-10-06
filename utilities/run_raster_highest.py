import subprocess
import shlex
import pdgstaging  # to get filepaths

import PRODUCTION_IWP_CONFIG

IWP_CONFIG = PRODUCTION_IWP_CONFIG.IWP_CONFIG


def run_raster_higest():

    # trying 20k with 5 CPU nodes was too much!
    BATCH_SIZE = 8_000

    print(f"Collecting all STAGED files from `{IWP_CONFIG['dir_staged']}`...")
    stager = pdgstaging.TileStager(IWP_CONFIG, check_footprints=False)
    stager.tiles.add_base_dir("output_of_staging", IWP_CONFIG["dir_staged"], ".gpkg")
    staged_paths = stager.tiles.get_filenames_from_dir(base_dir="output_of_staging")

    print("Total files:", len(staged_paths))
    print("Total batches (runs required):", len(staged_paths) // BATCH_SIZE)

    for i in range(0, len(staged_paths), BATCH_SIZE):
        start_idx = i
        end_idx = i + BATCH_SIZE
        print(f"Starting files index {i} to {i+BATCH_SIZE}")
        subprocess.run(
            f"python IN_PROGRESS_VIZ_WORKFLOW.py -s {start_idx} -e {end_idx}",
            check=True,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )  # capture_output=True,

        # rsync results to server, after each itr :)
        # Warning: deletes source files after rsync.
        subprocess.run(
            shlex.split(
                "/u/kastanday/viz/viz-workflow/utilities/rsync_merge_raster_to_scratch.py"
            ),
            shell=True,
        )

    print("Completed all batches. Exiting.")


if __name__ == "__main__":
    run_raster_higest()
