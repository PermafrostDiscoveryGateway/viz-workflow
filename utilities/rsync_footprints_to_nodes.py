# import os
# import subprocess
# import time
from subprocess import PIPE, Popen

''' get hostnames from slurm file '''
# /u/kastanday/viz/viz-workflow/slurm/nodes_array.txt
with open('/u/kastanday/viz/viz-workflow/slurm/nodes_array.txt', 'r') as f:
  hostnames = f.read().splitlines()

print("Syncing footprints to nodes:\n\t", '\n\t'.join(hostnames))

count = 0
file_count = 0
for hostname in hostnames:  
  ssh = ['ssh', f'{hostname}',]
  # rm = ['rm', '-rf', '/tmp/v3_viz_output' ]
  # rsync = ["ray stop && conda activate v2_full_viz_pipeline && ray start --head --node-ip-address=141.142.145.101 --port=6379 --dashboard-host 0.0.0.0" ]
  # rsync = ["conda activate v2_full_viz_pipeline && ray start --address '141.142.145.101:6379'" ]
  
  # SYNC FOOTPRINTS TO COMPUTE NOTES
  rsync = ['rsync', '-r', '--update', '--delete', '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/footprints/staged_footprints', '/tmp' ]
  
  # sync geotiff to /scratch
  # rsync = ['rsync', '-r', '--update', '/tmp/v3_viz_output/geotiff/WorldCRS84Quad/15/', '/scratch/bbki/kastanday/maple_data_xsede_bridges2/v3_viz_output/merged_geotiff_sep9/WorldCRS84Quad/15' ]
  # sync web_tiles to /scratch
  # rsync = ['rsync', '-r', '--update', '/tmp/_viz_output/3d_tiles/', '/scratch/bbki/kastanday/maple_data_xsede_bridges2/v3_viz_output/3d_tiles_sep_23' ]
  # rsync = ['find /tmp/v3_viz_output/web_tiles/ -type f | wc -l', ]
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
  