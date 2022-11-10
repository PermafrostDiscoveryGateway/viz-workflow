
# import os
import subprocess
import time
from subprocess import PIPE, Popen

''' get hostnames from slurm file '''
# /u/kastanday/viz/viz-workflow/slurm/nodes_array.txt
with open('/u/kastanday/viz/viz-workflow/slurm/nodes_array.txt', 'r') as f:
  hostnames = f.read().splitlines()

print("Moving geotiffs to main server, from worker nodes\n\t", '\n\t'.join(hostnames))

# Warning: Delets source files after rsync. 
DESTINATION = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/viz_pipline_outputs/v7_debug_viz_output/staged/cn094/geotiff'
SOURCE = '/tmp/v7_debug_viz_output/geotiff/'

count = 0
for hostname in hostnames:  
  # mkdir then sync
  mkdir = ['mkdir', '-p', DESTINATION]
  process = Popen(mkdir, stdin=PIPE, stdout=PIPE, stderr=PIPE)
  time.sleep(0.2)
  
  ssh = ['ssh', f'{hostname}',]
  rsync = ['rsync', '--remove-source-files', '-r', '--update', SOURCE, DESTINATION]
  cmd = ssh + rsync
  print(f"'{count} of {len(hostnames)}'. running command: {cmd}")
  count += 1
  process = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)

print("All jobs launched! They will work in the background WITHOUT stdout printing. ")

# otpional improvement
# shlex.split(s)  -- turn cmd line args into a list. 
