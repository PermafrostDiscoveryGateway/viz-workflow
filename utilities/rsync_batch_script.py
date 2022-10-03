
# import os
import subprocess
import time
from subprocess import PIPE, Popen

hostnames = [
'gpub074',
'gpub075',
'gpub076',
'gpub077',
'gpub078',
]

count = 0
for hostname in hostnames:  
  ssh = ['ssh', f'{hostname}',]
  # to use ssh in rsync (over a remote sheel) use the following: `rsync -rv --rsh=ssh hostname::module /dest``
  # see https://manpages.ubuntu.com/manpages/focal/en/man1/rsync.1.html (USING RSYNC-DAEMON FEATURES VIA A REMOTE-SHELL CONNECTION)
  
  rsync = ['rsync', '-rv', '--update', '/tmp/v4_viz_output/staged/WorldCRS84Quad/', f'/scratch/bbki/kastanday/maple_data_xsede_bridges2/v3_viz_output/{hostname}']
  cmd = ssh + rsync
  print(f"'{count} of {len(hostnames)}'. running command: {cmd}")
  count += 1
  
  # Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
  
  mkdir = ['mkdir', '-p', f'/scratch/bbki/kastanday/maple_data_xsede_bridges2/v3_viz_output/{hostname}']
  
  process = Popen(mkdir, stdin=PIPE, stdout=PIPE, stderr=PIPE)
  time.sleep(0.2)
  
  process = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
  # stdout, stderr = process.communicate()
  # print(stdout)
  # print(stderr)  
  # time.sleep(0.5)
  # break

print("All jobs launched! They will work in the background WITHOUT stdout printing. ")

# otpional improvement
# shlex.split(s)  -- turn cmd line args into a list. 
