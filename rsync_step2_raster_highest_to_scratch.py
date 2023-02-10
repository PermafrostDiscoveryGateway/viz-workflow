# import os
import subprocess
import time
from subprocess import PIPE, Popen
import PRODUCTION_IWP_CONFIG
IWP_CONFIG = PRODUCTION_IWP_CONFIG.IWP_CONFIG
# set config properties for current context
#IWP_CONFIG['dir_geotiff'] = IWP_CONFIG['dir_geotiff_local'] # switched IWP_CONFIG['dir_geotiff'] to be the LOCAL path in the config, no longer necesary to have IWP_CONFIG['dir_geotiff_local']
SOURCE = IWP_CONFIG['dir_geotiff']
IWP_CONFIG['dir_geotiff'] = IWP_CONFIG['dir_geotiff_remote']
DESTINATION = IWP_CONFIG['dir_geotiff']

# define user on Delta, avoid writing files to other user's dir
user = subprocess.check_output("whoami").strip().decode("ascii")

''' get hostnames from slurm file '''
with open(f'/u/{user}/viz-workflow/slurm/nodes_array.txt', 'r') as f:
  hostnames = f.read().splitlines()

#print("Syncing RasterHighest from /tmp to /scratch:\n\t", '\n\t'.join(hostnames))
print(f"Syncing RasterHighest from the /tmp on each node to the same destination: {DESTINATION}")

# create geotiff dir at /scratch/bbou/{user}/{output_subdir}/geotiff/
# because /scratch needs the directories to be manually created before 
# populating with files
mkdir = ['mkdir', '-p', DESTINATION]
process = Popen(mkdir, stdin=PIPE, stdout=PIPE, stderr=PIPE)
time.sleep(0.2)

# iterate through each node, transfer the geotiff files to the same destination dir 
# because ALL RASTER HIGHEST NEED TO BE IN THE SAME SUBDIR TO CORRECTLY EXECUTE THE RASTER LOWER STEP
count = 0
for hostname in hostnames:  
  # switch into the node
  ssh = ['ssh', hostname,]
  # rsync all files from that node's /tmp/geotiff to the same dir: /scratch/bbou/{user}/{output_subdir}/geotiff/
  rsync = ['rsync', '-r', '--update', SOURCE, DESTINATION]
  cmd = ssh + rsync
  print(f"'{count} of {len(hostnames)}'. running command: {cmd}")
  count += 1

  process = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)

print("All jobs launched! They will work in the background WITHOUT stdout printing. ")

# OLD CODE THAT IS WRONG BECAUSE WE DO NOT WANT HOSTNAMES TO BE SUBDIRS OF THE GEOTIFF DIR IN SCRATCH BC 
# ALL RASTER HIGHEST NEED TO BE IN THE SAME SUBDIR TO CORRECTLY EXECUTE THE RASTER LOWER STEP
# count = 0
# for hostname in hostnames:  
#   # to use ssh in rsync (over a remote sheel) use the following: `rsync -rv --rsh=ssh hostname::module /dest``
#   # see https://manpages.ubuntu.com/manpages/focal/en/man1/rsync.1.html (USING RSYNC-DAEMON FEATURES VIA A REMOTE-SHELL CONNECTION)

#   # mkdir then sync
#   mkdir = ['mkdir', '-p', f'{DESTINATION}{hostname}']
#   process = Popen(mkdir, stdin=PIPE, stdout=PIPE, stderr=PIPE)
#   time.sleep(0.2)

#   ssh = ['ssh', f'{hostname}',]
#   rsync = ['rsync', '-r', '--update', SOURCE, f'{DESTINATION}{hostname}']
#   cmd = ssh + rsync
#   print(f"'{count} of {len(hostnames)}'. running command: {cmd}")
#   count += 1

#   process = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)

# print("All jobs launched! They will work in the background WITHOUT stdout printing. ")

# otpional improvement
# shlex.split(s)  -- turn cmd line args into a list. 