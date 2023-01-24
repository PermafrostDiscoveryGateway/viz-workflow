from datetime import datetime
import pathlib
#######################
#### Change me 😁  ####
#######################
# ALWAYS include the tailing slash "/"
# BASE_DIR_OF_INPUT = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/'   # The output data of MAPLE. Which is the input data for STAGING.
# BASE_DIR_OF_INPUT = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/high_ice/'   # The output data of MAPLE. Which is the input data for STAGING.


import subprocess
whoami_username = subprocess.run(['whoami'], capture_output=True, text=True).stdout.strip()

# Use Elias' new shape files (cleaned. Oct 31, 2022)
INPUT               = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/glacier_water_cleaned_shp/high_ice/'   # The output data of MAPLE. Which is the input data for STAGING.
OUTPUT              = f'/scratch/bbki/{whoami_username}/maple_data_xsede_bridges2/viz_pipline_outputs/'

# TODO: finish thinking about all necessary paths to for full 100% automation.

FOOTPRINTS_REMOTE   = '/scratch/bbki/thiessenbock/pdg/staged_footprints/high_ice/'
FOOTPRINTS_LOCAL    = '/tmp/staged_footprints/'

STAGING_REMOTE      = OUTPUT  + 'staged/'
STAGING_LOCAL       = OUTPUT  + 'staged/'

GEOTIFF_REMOTE      = pathlib.Path(OUTPUT) / pathlib.Path('merged_geotiff_sep9')
GEOTIFF_LOCAL       = '/tmp/v7_debug_viz_output/' + 'geotiff/'

WEBTILE             = OUTPUT + 'web_tiles/'
THREE_D             = OUTPUT + '3d_tiles/'

# Convenience for little test runs. Change me 😁  
ONLY_SMALL_TEST_RUN = False                              # For testing, this ensures only a small handful of files are processed.
TEST_RUN_SIZE       = 10_000                              # Number of files to pre processed during testing (only effects testing)
##############################
#### END OF Change me 😁  ####
##############################

""" FINAL config is exporred here, and imported in the IPW Workflow python file. """
IWP_CONFIG = {
  "deduplicate_clip_to_footprint": True,
  "dir_output": str(OUTPUT),  # TODO: WRAP WITH STRINGGGG
  "dir_input": INPUT,
  "ext_input": ".shp",
  "dir_footprints": FOOTPRINTS_PATH,
  "dir_geotiff": GEOTIFF_PATH,
  "dir_web_tiles": WEBTILE,
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
  "deduplicate_clip_to_footprint": True
}
