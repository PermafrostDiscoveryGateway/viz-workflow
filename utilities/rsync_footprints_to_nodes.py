# import os
import subprocess

# import time
from subprocess import PIPE, Popen
import PRODUCTION_IWP_CONFIG

IWP_CONFIG = PRODUCTION_IWP_CONFIG.IWP_CONFIG

# define user on Delta, avoid writing files to other user's dir
user = subprocess.check_output("whoami").strip().decode("ascii")

# define where footprints are pulled from, using the correct property in config
SOURCE = IWP_CONFIG["dir_footprints_remote"]
# define where footprints should be transferred to, using the correct property in config
DESTINATION = IWP_CONFIG["dir_footprints_local"]

""" get hostnames from slurm file """
with open(f"/u/{user}/viz-workflow/slurm/nodes_array.txt", "r") as f:
    hostnames = f.read().splitlines()

print("Syncing footprints to nodes:\n\t", "\n\t".join(hostnames))

count = 0
file_count = 0
for hostname in hostnames:
    ssh = [
        "ssh",
        f"{hostname}",
    ]
    # rm = ['rm', '-rf', '/tmp/v3_viz_output' ]
    # rsync = ["ray stop && conda activate v2_full_viz_pipeline && ray start --head --node-ip-address=141.142.145.101 --port=6379 --dashboard-host 0.0.0.0" ]
    # rsync = ["conda activate v2_full_viz_pipeline && ray start --address '141.142.145.101:6379'" ]

    # SYNC FOOTPRINTS TO COMPUTE NOTES
    # -r = recursive, so no need to mkdir -p before transfer
    rsync = ["rsync", "-r", "--update", "--delete", SOURCE, DESTINATION]
    cmd = ssh + rsync
    print(f"'{count} of {len(hostnames)}'. running command: {cmd}")
    count += 1

    process = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    # stdout, stderr = process.communicate()
    # print(stdout)
    # print(stderr)
    # file_count += int(stdout)
    # break
print(f"Total files: {file_count}")
print("All jobs launched! They will work in the background WITHOUT stdout printing.")


# os.system("rsync -ar --dry-run kastanday@ip:/opt/data/file")
# subprocess.call(['rsync', '-avz', '--min-size=1', '--include=*.txt', '--exclude=*', Sourcedir, Destdir ])
