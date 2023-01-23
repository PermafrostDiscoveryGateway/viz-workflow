
# import os
import subprocess
import time
from subprocess import PIPE, Popen

# define user on Delta, avoid writing files to other user's dir
user = subprocess.check_output("whoami").strip().decode("ascii")
output_subdir = '2023-01-20'

''' get hostnames from slurm file '''
with open(f'/u/{user}/viz-workflow/slurm/nodes_array.txt', 'r') as f:
  hostnames = f.read().splitlines()

print("Moving geotiffs to main server, from worker nodes\n\t", '\n\t'.join(hostnames))

# Warning: Deletes source files after rsync. 
SOURCE = '/tmp/geotiff/'
DESTINATION = f'/scratch/bbou/{user}/IWP/output/{output_subdir}/geotiff'

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
