from datetime import datetime
#######################
#### Change me 😁  ####
#######################
# ALWAYS include the tailing slash "/"
# BASE_DIR_OF_INPUT = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/'   # The output data of MAPLE. Which is the input data for STAGING.
# BASE_DIR_OF_INPUT = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/high_ice/'   # The output data of MAPLE. Which is the input data for STAGING.

# Use Elias' new shape files (cleaned. Oct 31, 2022)
BASE_DIR_OF_INPUT = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/glacier_water_cleaned_shp/high_ice/'   # The output data of MAPLE. Which is the input data for STAGING.
# BASE_DIR_OF_INPUT = '/ime/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/aug_28_alaska_high_only/'   # The output data of MAPLE. Which is the input data for STAGING.
# FOOTPRINTS_PATH   = BASE_DIR_OF_INPUT + 'footprints/staged_footprints/'
# FOOTPRINTS_PATH = '/tmp/staged_footprints/'
FOOTPRINTS_PATH = '/scratch/bbki/thiessenbock/pdg/staged_footprints/high_ice/'

# OUTPUT            = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/aug_28_alaska_high_only/viz_output/'       # Dir for results. High I/O is good.
# OUTPUT            = '/tmp/v7_debug_viz_output/'       # Dir for results. High I/O is good.
OUTPUT            = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/viz_pipline_outputs/v7_debug_viz_output/staged/cn094/'       # Dir for results. High I/O is good.
# OUTPUT            = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/viz_output/'
# OUTPUT            = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_13_FULL_SINGLE_RUN/gpub052/july_13_fifteennode/'       # Dir for results.
# OUTPUT            = '/ime/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_24_v2/'       # Dir for results.
OUTPUT_OF_STAGING = OUTPUT  + 'staged/'              # Output dirs for each sub-step
# OUTPUT_OF_STAGING = '/scratch/bbki/thiessenbock/pdg/staged/gpub044'
# OUTPUT_OF_STAGING = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/v1_debug_viz_output/staged/merged/'
# GEOTIFF_PATH      = OUTPUT + 'geotiff/'
GEOTIFF_PATH      = '/tmp/v7_debug_viz_output/' + 'geotiff/'
# GEOTIFF_PATH      = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/v3_viz_output/merged_geotiff_sep9/'
WEBTILE_PATH      = OUTPUT + 'web_tiles/'
THREE_D_PATH      = OUTPUT + '3d_tiles/'

# Convenience for little test runs. Change me 😁  
ONLY_SMALL_TEST_RUN = True                            # For testing, this ensures only a small handful of files are processed.
TEST_RUN_SIZE       = 10_000                              # Number of files to pre processed during testing (only effects testing)
##############################
#### END OF Change me 😁  ####
##############################

""" FINAL config is exporred here, and imported in the IPW Workflow python file. """
IWP_CONFIG = {"deduplicate_clip_to_footprint": True, "dir_output": OUTPUT, "dir_input": BASE_DIR_OF_INPUT,"ext_input": ".shp","dir_footprints": FOOTPRINTS_PATH,"dir_geotiff": GEOTIFF_PATH,"dir_web_tiles": WEBTILE_PATH,"dir_staged": OUTPUT_OF_STAGING,"filename_staging_summary": OUTPUT_OF_STAGING + "staging_summary.csv","filename_rasterization_events": GEOTIFF_PATH + "raster_events.csv","filename_rasters_summary": GEOTIFF_PATH + "raster_summary.csv","version": datetime.now().strftime("%B%d,%Y"),"simplify_tolerance": 0.1,"tms_id": "WorldCRS84Quad","z_range": [0, 15],"geometricError": 57,"z_coord": 0,"statistics": [    {        "name": "iwp_count",        "weight_by": "count",        "property": "centroids_per_pixel",        "aggregation_method": "sum",        "resampling_method": "sum",        "val_range": [0, None],        "palette": ["#66339952", "#d93fce", "#ffcc00"],        "nodata_val": 0,        "nodata_color": "#ffffff00"    },    {        "name": "iwp_coverage",        "weight_by": "area",        "property": "area_per_pixel_area",        "aggregation_method": "sum",        "resampling_method": "average",        "val_range": [0, 1],        "palette": ["#66339952", "#ffcc00"],        "nodata_val": 0,        "nodata_color": "#ffffff00"    },],"deduplicate_at": ["raster", "3dtiles"],"deduplicate_keep_rules": [["Date", "larger"]],"deduplicate_method": "footprints",}