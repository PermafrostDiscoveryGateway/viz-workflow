
# import os
import subprocess
import time
from subprocess import PIPE, Popen

''' get hostnames from slurm file '''
# /u/kastanday/viz/viz-workflow/slurm/nodes_array.txt
with open('/u/kastanday/viz/viz-workflow/slurm/nodes_array.txt', 'r') as f:
  hostnames = f.read().splitlines()

print("Syncing footprints to nodes:\n\t", '\n\t'.join(hostnames))

count = 0
for hostname in hostnames:  
  # to use ssh in rsync (over a remote sheel) use the following: `rsync -rv --rsh=ssh hostname::module /dest``
  # see https://manpages.ubuntu.com/manpages/focal/en/man1/rsync.1.html (USING RSYNC-DAEMON FEATURES VIA A REMOTE-SHELL CONNECTION)
  
  # mkdir on scratch
  # mkdir = ['mkdir', '-p', f'/scratch/bbki/kastanday/maple_data_xsede_bridges2/v6_debug_viz_output/staged/{hostname}']
  # process = Popen(mkdir, stdin=PIPE, stdout=PIPE, stderr=PIPE)
  # time.sleep(0.2)
  
  ## RSYNC TO /scratch
  ssh = ['ssh', f'{hostname}',]
  # rsync = ['rsync', '-r', '--update', '/tmp/v6_debug_viz_output/staged/', f'/scratch/bbki/kastanday/maple_data_xsede_bridges2/v6_debug_viz_output/staged/{hostname}']
  rsync = ['rsync', '/tmp/{hostname}_staged.tar.zstd', f'/scratch/bbki/kastanday/maple_data_xsede_bridges2/v6_debug_viz_output/staged']
  cmd = ssh + rsync
  print(f"'{count} of {len(hostnames)}'. running command: {cmd}")
  count += 1
  process = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
  
  ## Compress 'staged' on each node's tmp dir, for faster copy later. 
  # tar_and_zstd = [ f'tar cf - /tmp/v6_debug_viz_output | pv | zstd -2 > /tmp/{hostname}_staged.tar.zstd' ]
  # cmd = ssh + tar_and_zstd
  # process_2 = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
  
  # stdout, stderr = process.communicate()
  # print(stdout)
  # print(stderr)  
  # time.sleep(0.5)
  # break

print("All jobs launched! They will work in the background WITHOUT stdout printing. ")

# otpional improvement
# shlex.split(s)  -- turn cmd line args into a list. 
