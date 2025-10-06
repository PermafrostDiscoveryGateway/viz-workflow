# import os
import time
from datetime import datetime
import subprocess
from subprocess import PIPE, Popen
import pprint

# -----------------------------------------------------
# CHOOSE ONE OF THE FOLLOWING IMPORT & CONFIG CHUNKS

# for processing IWP:
# import PRODUCTION_IWP_CONFIG
# IWP_CONFIG = PRODUCTION_IWP_CONFIG.IWP_CONFIG
# IWP_CONFIG2 = IWP_CONFIG.copy()

# for testing branches with IWP data:
# import branch_testing_iwp_config
# IWP_CONFIG = branch_testing_iwp_config.IWP_CONFIG
# IWP_CONFIG2 = IWP_CONFIG.copy()

# for infrastructure data:
import infrastructure_config

CONFIG = infrastructure_config.CONFIG
CONFIG2 = CONFIG.copy()
# -----------------------------------------------------

# set config properties for current context
CONFIG2["dir_staged"] = CONFIG2["dir_staged_local"]
SOURCE = CONFIG2["dir_staged"]
CONFIG2["dir_staged"] = CONFIG2["dir_staged_remote"]
DESTINATION = CONFIG2["dir_staged"]

print("Using config: ")
pprint.pprint(CONFIG2)

# define user on Delta, avoid writing files to other user's dir
user = subprocess.check_output("whoami").strip().decode("ascii")
# define desired location for output files within user dir
# ensures a new subfolder every run as long as new run is not started within same day as last run
# output_subdir = datetime.now().strftime("%b-%d-%y")
# don't use subprocess to retrieve date for subdir because runs might span over 2 days if they go overnight
# output_subdir = "iwp_testRun_20230131"
# output_subdir = '2023-01-20'

""" get hostnames from slurm file """
with open(f"/u/{user}/viz-workflow/slurm/nodes_array.txt", "r") as f:
    hostnames = f.read().splitlines()

print("Syncing staged files to nodes:\n\t", "\n\t".join(hostnames))

count = 0
for hostname in hostnames:
    # to use ssh in rsync (over a remote sheel) use the following: `rsync -rv --rsh=ssh hostname::module /dest``
    # see https://manpages.ubuntu.com/manpages/focal/en/man1/rsync.1.html (USING RSYNC-DAEMON FEATURES VIA A REMOTE-SHELL CONNECTION)

    # mkdir then sync
    mkdir = ["mkdir", "-p", f"{DESTINATION}{hostname}"]
    process = Popen(mkdir, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    time.sleep(0.2)

    ssh = [
        "ssh",
        f"{hostname}",
    ]  # from juliet: does this have to end in comma cause it is concatenated with rsync command below?
    # old command before we separated paths into LOCAL and REMOTE:
    # rsync = ['rsync', '-r', '--update', '/tmp/staged/', f'/scratch/bbou/{user}/IWP/output/{output_subdir}/staged/{hostname}']
    # new command now that we separated paths into LOCAL and REMOTE:
    rsync = ["rsync", "-r", "--update", SOURCE, f"{DESTINATION}{hostname}"]
    cmd = ssh + rsync
    print(f"'{count} of {len(hostnames)}'. running command: {cmd}")
    count += 1

    process = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)

print("All jobs launched! They will work in the background WITHOUT stdout printing. ")

# otpional improvement
# shlex.split(s)  -- turn cmd line args into a list.
