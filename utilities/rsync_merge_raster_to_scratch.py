# import os
import subprocess
import time
from subprocess import PIPE, Popen
import pprint
import PRODUCTION_IWP_CONFIG

IWP_CONFIG = PRODUCTION_IWP_CONFIG.IWP_CONFIG

# define user on Delta, avoid writing files to other user's dir
user = subprocess.check_output("whoami").strip().decode("ascii")
# output_subdir = '2023-01-20'

""" get hostnames from slurm file """
with open(f"/u/{user}/viz-workflow/slurm/nodes_array.txt", "r") as f:
    hostnames = f.read().splitlines()

print("Moving geotiffs to main server, from worker nodes\n\t", "\n\t".join(hostnames))

# Warning: Deletes source files after rsync.
# SOURCE = '/tmp/geotiff/'
# DESTINATION = f'/scratch/bbou/{user}/IWP/output/{output_subdir}/geotiff'

# define where geotiffs are pulled from, using the correct property in config
IWP_CONFIG["dir_geotiff"] = IWP_CONFIG["dir_geotiff_local"]
SOURCE = IWP_CONFIG["dir_geotiff"]
# define where geotiffs should be transferred to, using the correct property in config
IWP_CONFIG["dir_geotiff"] = IWP_CONFIG["dir_geotiff_remote"]
DESTINATION = IWP_CONFIG["dir_geotiff"]

# mkdir -p = make directories and parent directories in destination first
# note from Juliet: do we need the mkdir command if we already have -r (recursive)
# in the rsync command?
mkdir = ["mkdir", "-p", DESTINATION]
process = Popen(mkdir, stdin=PIPE, stdout=PIPE, stderr=PIPE)
time.sleep(0.2)

count = 0
for hostname in hostnames:
    # ssh into the node, and rsync!
    ssh = [
        "ssh",
        f"{hostname}",
    ]
    rsync = ["rsync", "-r", "--update", SOURCE, DESTINATION]
    cmd = ssh + rsync
    print(f"'{count} of {len(hostnames)}'. running command: {cmd}")
    count += 1
    process = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)

print("All jobs launched! They will work in the background WITHOUT stdout printing. ")
