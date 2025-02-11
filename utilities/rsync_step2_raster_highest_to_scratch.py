# import os
import subprocess
import time
from subprocess import PIPE, Popen

# -----------------------------------------------------
# CHOOSE ONE OF THE FOLLOWING IMPORT & CONFIG CHUNKS

# for processing IWP:
# import PRODUCTION_IWP_CONFIG
# IWP_CONFIG = PRODUCTION_IWP_CONFIG.IWP_CONFIG

# for testing branches with IWP data:
# import branch_testing_iwp_config
# IWP_CONFIG = branch_testing_iwp_config.IWP_CONFIG

# for infrastructure data:
import infrastructure_config

CONFIG = infrastructure_config.CONFIG
# -----------------------------------------------------

# set config properties for current context
CONFIG["dir_geotiff"] = CONFIG["dir_geotiff_local"]
SOURCE = CONFIG["dir_geotiff"]
CONFIG["dir_geotiff"] = CONFIG["dir_geotiff_remote"]
DESTINATION = CONFIG["dir_geotiff"]

# define user on Delta, avoid writing files to other user's dir
user = subprocess.check_output("whoami").strip().decode("ascii")

""" get hostnames from slurm file """
with open(f"/u/{user}/viz-workflow/slurm/nodes_array.txt", "r") as f:
    hostnames = f.read().splitlines()

# print("Syncing RasterHighest from /tmp to /scratch:\n\t", '\n\t'.join(hostnames))
print(
    f"Syncing RasterHighest from the /tmp on each node to the same destination: {DESTINATION}"
)

# create geotiff dir at /scratch/bbou/{user}/{output_subdir}/geotiff/
# because /scratch needs the directories to be manually created before
# populating with files
mkdir = ["mkdir", "-p", DESTINATION]
process = Popen(mkdir, stdin=PIPE, stdout=PIPE, stderr=PIPE)
time.sleep(0.2)

# iterate through each node, transfer the geotiff files to the same destination dir
# because ALL RASTER HIGHEST NEED TO BE IN THE SAME SUBDIR TO CORRECTLY EXECUTE THE RASTER LOWER STEP
count = 0
for hostname in hostnames:
    # switch into the node
    ssh = [
        "ssh",
        hostname,
    ]
    # rsync all files from that node's /tmp/geotiff to the same dir: /scratch/bbou/{user}/{output_subdir}/geotiff/
    rsync = ["rsync", "-r", "--update", SOURCE, DESTINATION]
    cmd = ssh + rsync
    print(f"'{count} of {len(hostnames)}'. running command: {cmd}")
    count += 1

    process = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)

print("All jobs launched! They will work in the background WITHOUT stdout printing. ")
