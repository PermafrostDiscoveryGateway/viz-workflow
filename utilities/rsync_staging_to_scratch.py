
# import os
import subprocess
import time
from subprocess import PIPE, Popen

hostnames = [
'gpub088',
'gpub090',
'gpub091',
'gpub092',
'gpub093',
'gpub094',
'gpub095',
'gpub096',
'gpub097',
'gpub098',
]

count = 0
for hostname in hostnames:  
  # to use ssh in rsync (over a remote sheel) use the following: `rsync -rv --rsh=ssh hostname::module /dest``
  # see https://manpages.ubuntu.com/manpages/focal/en/man1/rsync.1.html (USING RSYNC-DAEMON FEATURES VIA A REMOTE-SHELL CONNECTION)
  
  # mkdir then sync
  mkdir = ['mkdir', '-p', f'/scratch/bbki/kastanday/maple_data_xsede_bridges2/v1_debug_viz_output/staged/{hostname}']
  process = Popen(mkdir, stdin=PIPE, stdout=PIPE, stderr=PIPE)
  time.sleep(0.2)
  
  ssh = ['ssh', f'{hostname}',]
  rsync = ['rsync', '-r', '--update', '/tmp/v1_debug_viz_output/staged/', f'/scratch/bbki/kastanday/maple_data_xsede_bridges2/v1_debug_viz_output/staged/{hostname}']
  cmd = ssh + rsync
  print(f"'{count} of {len(hostnames)}'. running command: {cmd}")
  count += 1
  
  # Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
  
  
  process = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
  # stdout, stderr = process.communicate()
  # print(stdout)
  # print(stderr)  
  # time.sleep(0.5)
  # break

print("All jobs launched! They will work in the background WITHOUT stdout printing. ")

# otpional improvement
# shlex.split(s)  -- turn cmd line args into a list. 
