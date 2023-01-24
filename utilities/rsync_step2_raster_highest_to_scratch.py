
# import os
import subprocess
import time
from subprocess import PIPE, Popen

''' get hostnames from slurm file '''
# /u/kastanday/viz/viz-workflow/slurm/nodes_array.txt
with open('/u/kastanday/viz/viz-workflow/slurm/nodes_array.txt', 'r') as f:
  hostnames = f.read().splitlines()

print("Syncing RasterHighest from /tmp to /scratch:\n\t", '\n\t'.join(hostnames))

count = 0
for hostname in hostnames:  
  # to use ssh in rsync (over a remote sheel) use the following: `rsync -rv --rsh=ssh hostname::module /dest``
  # see https://manpages.ubuntu.com/manpages/focal/en/man1/rsync.1.html (USING RSYNC-DAEMON FEATURES VIA A REMOTE-SHELL CONNECTION)
  
  # mkdir then sync
  mkdir = ['mkdir', '-p', f'/scratch/bbki/kastanday/maple_data_xsede_bridges2/viz_pipline_outputs/v7_debug_viz_output/staged/{hostname}']
  process = Popen(mkdir, stdin=PIPE, stdout=PIPE, stderr=PIPE)
  time.sleep(0.2)
  
  ssh = ['ssh', f'{hostname}',]
  rsync = ['rsync', '-r', '--update', '/tmp/v7_debug_viz_output/staged/', f'/scratch/bbki/kastanday/maple_data_xsede_bridges2/viz_pipline_outputs/v7_debug_viz_output/staged/{hostname}']
  cmd = ssh + rsync
  print(f"'{count} of {len(hostnames)}'. running command: {cmd}")
  count += 1
  
  process = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)

print("All jobs launched! They will work in the background WITHOUT stdout printing. ")

# otpional improvement
# shlex.split(s)  -- turn cmd line args into a list. 
