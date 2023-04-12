from datetime import datetime
import subprocess
#######################
#### Change me 😁  ####
#######################
# ALWAYS include the tailing slash "/"

# define user on Delta, avoid writing files to other user's dir
user = subprocess.check_output("whoami").strip().decode("ascii")
#head_node = 'cn___/'
head_node = 'gpub086'
# define desired location for output files within user dir
# ensures a new subfolder every run as long as new run is not started within same day as last run
# following path is the output subdir for test run, using just on subdir of the alaska files that is only ~8% of the Alaska dir, 23.5 GB
output_subdir = 'lake_change/output/utm_32640_20230411'
#output_subdir = datetime.now().strftime("%b-%d-%y")
# don't use subprocess to retrieve date for subdir because runs might span over 2 days if they go overnight

##############################
#### END OF Change me 😁  ####
##############################

# input path for all data:
INPUT = '/scratch/bbou/julietcohen/lake_change/input/sample/'

# following path is the OUTPUT for test run, using just on subdir of the alaska files that is only 7.78% of the Alaska dir, 45.57 GB
OUTPUT  = f'/scratch/bbou/{user}/{output_subdir}/' # Dir for results. High I/O is good.

STAGING_LOCAL = '/tmp/staged/'
STAGING_REMOTE = OUTPUT  + 'staged/'
STAGING_REMOTE_MERGED = STAGING_REMOTE + head_node

GEOTIFF_LOCAL = '/tmp/geotiff/'
GEOTIFF_REMOTE = OUTPUT + 'geotiff/' # Kastan used pathlib.Path(OUTPUT) / pathlib.Path('merged_geotiff_sep9') for this so if it errors try something similar
# check if need a variable GEOTIFF_REMOTE_MERGED after we finish the raster step successfully

#WEBTILE_LOCAL = '/tmp/web_tiles/' # we do not use /tmp for webtile step, it is unique in that way
WEBTILE_REMOTE = OUTPUT + 'web_tiles/'

#THREE_D_PATH      = OUTPUT + '3d_tiles/' # workflow does not accomodate 3d-tiling yet

""" FINAL config is exported here, and imported in the IPW Workflow python file. """
IWP_CONFIG = {
  "deduplicate_clip_to_footprint": False,
  "dir_output": OUTPUT, # base dir of all output, needs to change every run with definition of output_subdir
  "dir_input": INPUT, # base dir of all files to be staged
  "ext_input": ".gpkg",
  "dir_geotiff_remote": GEOTIFF_REMOTE, # we store geotiffs in /scratch after they are created so they are safe after the job concludes, and web-tiling can access all geotiffs in the same directory
  "dir_geotiff_local": GEOTIFF_LOCAL, # we write highest level geotiffs to /tmp then transfer to /scratch 
  "dir_web_tiles": WEBTILE_REMOTE, # we do not use /tmp for webtile step, it writes directly to /scratch
  "dir_staged_remote": STAGING_REMOTE, # we rsync the staged files to /scratch to merge, then rasterize and 3dtile with that merged dir
  "dir_staged_remote_merged": STAGING_REMOTE_MERGED, # input for raster highest after staged files have been merged
  "dir_staged_local": STAGING_LOCAL, # initially write staged files to /tmp so they write faster
  "filename_staging_summary": STAGING_REMOTE + "staging_summary.csv",
  "filename_rasterization_events": GEOTIFF_REMOTE + "raster_events.csv",
  "filename_rasters_summary": GEOTIFF_REMOTE + "raster_summary.csv",
  "version": datetime.now().strftime("%B%d,%Y"),
  "simplify_tolerance": 0.1,
  "tms_id": "WGS1984Quad",
  "z_range": [
    0,
    11
  ],
  "geometricError": 57,
  "z_coord": 0,
  "statistics": [
    {
      "name": "change_rate", # changed from "coverage"
      "weight_by": "area",
      "property": "ChangeRateGrowth_myr-1", # changed from "area_per_pixel_area", can also be property that is available in input data 
      "aggregation_method": "sum",  
      "resampling_method": "sum", # changed from "average"
      "val_range": [
        0,
        1
      ],
      "palette": ["#ff0000", # red
                  "#FF8C00", # DarkOrange
                  "#FFA07A", # LightSalmon
                  "#FFFF00", # yellow
                  "#66CDAA", # MediumAquaMarine
                  "#AFEEEE", # PaleTurquoise,
                  "#0000ff"], # blue
      "nodata_val": 0,
      "nodata_color": "#ffffff00"
    },
  ],
  "deduplicate_at": "raster",
  "deduplicate_keep_rules": [
    [
      "Date",
      "larger"
    ]
  ],
  "deduplicate_method": "neighbor",
  "deduplicate_keep_rules": [["staging_filename", "larger"]],
  "deduplicate_overlap_tolerance": 0.1,
  "deduplicate_overlap_both": False,
  "deduplicate_centroid_tolerance": None
}
