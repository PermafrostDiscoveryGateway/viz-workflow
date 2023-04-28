from datetime import datetime
import subprocess
#######################
#### Change me 😁  ####
#######################
# ALWAYS include the tailing slash "/"

# define user on Delta, avoid writing files to other user's dir
user = subprocess.check_output("whoami").strip().decode("ascii")
#head_node = 'cn063'
head_node = 'gpub074'
# define desired location for output files within user dir
# ensures a new subfolder every run as long as new run is not started within same day as last run
# following path is the output subdir for test run, using just on subdir of the alaska files that is only ~8% of the Alaska dir, 23.5 GB
output_subdir = 'IWP/output/iwp_testRun_20230428'
#output_subdir = datetime.now().strftime("%b-%d-%y")
# don't use subprocess to retrieve date for subdir because runs might span over 2 days if they go overnight

##############################
#### END OF Change me 😁  ####
##############################

# input path for all data:
#INPUT = '/scratch/bbou/julietcohen/IWP/input/2023-01-19/iwp_files/high/' # The output data of MAPLE. Which is the input data for STAGING.
#INPUT = '/scratch/bbou/julietcohen/IWP/input/2023-01-19/iwp_files/high/russia/226_227_iwp/'
INPUT = '/scratch/bbou/julietcohen/IWP/input/few_adjacent_russia/iwp/'

# following path is the OUTPUT for test run, using just on subdir of the alaska files that is only 7.78% of the Alaska dir, 45.57 GB
OUTPUT  = f'/scratch/bbou/{user}/{output_subdir}/' # Dir for results. High I/O is good.
# output path for all data, when it is available:
#OUTPUT  = f'/scratch/bbou/{user}/IWP/output/{output_subdir}/' # Dir for results. High I/O is good.

# footprints paths for all data:
FOOTPRINTS_LOCAL = '/tmp/staged_footprints/'
#FOOTPRINTS_REMOTE = '/scratch/bbou/julietcohen/IWP/footprint_files_with_date_20230119/high/'
#FOOTPRINTS_REMOTE = '/scratch/bbou/julietcohen/IWP/footprint_files_with_date_20230119/high/russia/226_227_iwp/'
FOOTPRINTS_REMOTE = '/scratch/bbou/julietcohen/IWP/input/few_adjacent_russia/footprints/'

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
  "deduplicate_clip_to_footprint": True,
  "dir_output": OUTPUT, # base dir of all output, needs to change every run with definition of output_subdir
  "dir_input": INPUT, # base dir of all .shp files to be staged
  "ext_input": ".shp",
  "ext_footprints": ".shp",
  "dir_footprints_remote": FOOTPRINTS_REMOTE, # the footprints start on /scratch before we transfer them to /tmp
  "dir_footprints_local": FOOTPRINTS_LOCAL, # we rsync footprints from /scratch to /tmp before we use them for deduplication
  "dir_geotiff_remote": GEOTIFF_REMOTE, # we store geotiffs in /scratch after they are created so they are safe after the job concludes, and web-tiling can access all geotiffs in the same directory
  "dir_geotiff_local": GEOTIFF_LOCAL, # we write highest level geotiffs to /tmp then transfer to /scratch 
  "dir_web_tiles": WEBTILE_REMOTE, # we do not use /tmp for webtile step, it writes directly to /scratch
  "dir_staged_remote": STAGING_REMOTE, # we rsync the staged files to /scratch to merge, then rasterize and 3dtile with that merged dir
  "dir_staged_remote_merged": STAGING_REMOTE_MERGED, # input for raster highest after staged files have been merged
  "dir_staged_local": STAGING_LOCAL, # initially write staged files to /tmp so they write faster
  "filename_staging_summary": STAGING_REMOTE + "staging_summary.csv",
  "filename_rasterization_events": GEOTIFF_REMOTE + "raster_events.csv",
  "filename_rasters_summary": GEOTIFF_REMOTE + "raster_summary.csv",
  #"filename_config": OUTPUT + "config",
  "version": datetime.now().strftime("%B%d,%Y"),
  "simplify_tolerance": 0.1,
  "tms_id": "WGS1984Quad",
  "z_range": [
    0,
    15
  ],
  "geometricError": 57,
  "z_coord": 0,
  "statistics": [
    {
      "name": "iwp_coverage",
      "weight_by": "area",
      "property": "area_per_pixel_area",
      "aggregation_method": "sum",
      "resampling_method": "average",
      "val_range": [
        0,
        1
      ],
      "palette": [
        "#66339952",
        "#ffcc00"
      ],
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
  "deduplicate_method": "footprints"
}
