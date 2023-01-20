from datetime import datetime
import subprocess
#######################
#### Change me 😁  ####
#######################
# ALWAYS include the tailing slash "/"

# define user on Delta, avoid writing files to other user's dir
user = subprocess.check_output("whoami").strip().decode("ascii")
# define desired location for output files within user dir
# ensures a new subfolder every run as long as new run is not started within same day as last run
#output_subdir = datetime.now().strftime("%b-%d-%y")
# don't use subprocess to retrieve date for subdir because runs might span over 2 days if they go overnight
output_subdir = '2023-01-20'

# Use Elias' new shape files
BASE_DIR_OF_INPUT = '/scratch/bbou/julietcohen/IWP/input/2023-01-19/.../high_ice/'   # The output data of MAPLE. Which is the input data for STAGING.
FOOTPRINTS_PATH = '/scratch/bbou/julietcohen/IWP/input/2023-01-19/.../high_ice/'
OUTPUT            = f'/scratch/bbou/{user}/IWP/output/{output_subdir}/'       # Dir for results. High I/O is good.
OUTPUT_OF_STAGING = OUTPUT  + 'staged/'              # Output dirs for each sub-step
GEOTIFF_PATH      = '/tmp/' + 'geotiff/'
WEBTILE_PATH      = OUTPUT + 'web_tiles/'
THREE_D_PATH      = OUTPUT + '3d_tiles/'

# Convenience for little test runs. Change me 😁  
ONLY_SMALL_TEST_RUN = True                            # For testing, this ensures only a small handful of files are processed.
TEST_RUN_SIZE       = 10_000                              # Number of files to pre processed during testing (only effects testing)
##############################
#### END OF Change me 😁  ####
##############################

""" FINAL config is exporred here, and imported in the IPW Workflow python file. """
IWP_CONFIG = {
  "deduplicate_clip_to_footprint": True,
  "dir_output": OUTPUT,
  "dir_input": BASE_DIR_OF_INPUT,
  "ext_input": ".shp",
  "dir_footprints": FOOTPRINTS_PATH,
  "dir_geotiff": GEOTIFF_PATH,
  "dir_web_tiles": WEBTILE_PATH,
  "dir_staged": OUTPUT_OF_STAGING,
  "filename_staging_summary": OUTPUT_OF_STAGING + "staging_summary.csv",
  "filename_rasterization_events": GEOTIFF_PATH + "raster_events.csv",
  "filename_rasters_summary": GEOTIFF_PATH + "raster_summary.csv",
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
      "name": "iwp_count",
      "weight_by": "count",
      "property": "centroids_per_pixel",
      "aggregation_method": "sum",
      "resampling_method": "sum",
      "val_range": [
        0, None
      ],
      "palette": [
        "#66339952",
        "#d93fce",
        "#ffcc00"
      ],
      "nodata_val": 0,
      "nodata_color": "#ffffff00"
    },
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
  "deduplicate_at": [
    "raster",
    "3dtiles"
  ],
  "deduplicate_keep_rules": [
    [
      "Date",
      "larger"
    ]
  ],
  "deduplicate_method": "footprints",
}
