from datetime import datetime
import subprocess
import numpy as np

# always include the tailing slash "/"
# define user on Delta, avoid writing files to other user's dir
user = subprocess.check_output("whoami").strip().decode("ascii")
head_node = "cn040/"
# head_node = 'gpub___'

INPUT = "/scratch/bbou/julietcohen/lake_change/input/time_series/annual/yr2021/"
output_subdir = "lake_change/output/time_series_annual/yr2021"
OUTPUT = f"/scratch/bbou/{user}/{output_subdir}/"

STAGING_LOCAL = "/tmp/staged/"
STAGING_REMOTE = OUTPUT + "staged/"
STAGING_REMOTE_MERGED = STAGING_REMOTE + head_node

GEOTIFF_LOCAL = "/tmp/geotiff/"
GEOTIFF_REMOTE = OUTPUT + "geotiff/"

WEBTILE_REMOTE = OUTPUT + "web_tiles/"

""" final config is exported here, and imported in the workflow python file. """
IWP_CONFIG = {
    "deduplicate_clip_to_footprint": False,
    "deduplicate_method": None,
    "deduplicate_at": None,
    "dir_output": OUTPUT,
    "dir_input": INPUT,
    "ext_input": ".gpkg",
    "dir_geotiff_remote": GEOTIFF_REMOTE,
    "dir_geotiff_local": GEOTIFF_LOCAL,
    "dir_web_tiles": WEBTILE_REMOTE,
    "dir_staged_remote": STAGING_REMOTE,
    "dir_staged_remote_merged": STAGING_REMOTE_MERGED,
    "dir_staged_local": STAGING_LOCAL,
    "filename_staging_summary": STAGING_REMOTE + "staging_summary.csv",
    "filename_rasterization_events": GEOTIFF_REMOTE + "raster_events.csv",
    "filename_rasters_summary": GEOTIFF_REMOTE + "raster_summary.csv",
    "version": datetime.now().strftime("%B%d,%Y"),
    "simplify_tolerance": 0.1,
    "tms_id": "WGS1984Quad",
    "z_range": [0, 12],
    "geometricError": 57,
    "z_coord": 0,
    "statistics": [
        {
            "name": "permanent_water",
            "weight_by": "area",
            "property": "permanent_water",
            "aggregation_method": "max",
            "resampling_method": "mode",
            "val_range": [
                0,
                # 6088.89 # 99.99th percentile for 2017, this shows the best diversity for perm water
                # 6105.43 # 99.99th percentile for 2018
                # 6103.33 # 99.99th percentile for 2019
                # 6093.07 # 99.99th percentile for 2020
                6071.56,  # 99.99th percentile for 2021
            ],
            "palette": ["#1be3ee", "#1b85ee", "#1b22ee"],  # blues
            "nodata_val": 0,
            "nodata_color": "#ffffff00",
        },
        {
            "name": "seasonal_water",
            "weight_by": "area",
            "property": "seasonal_water",
            "aggregation_method": "max",
            "resampling_method": "mode",
            "val_range": [
                0,
                # 2.66 # 95th percentile for 2017
                # 2.47 # 95th percentile for 2018
                # 2.64 # 95th percentile for 2019
                # 3.01 # 95th percentile for 2020
                2.86,  # 95th percentile for 2021
            ],
            "palette": ["#f000d8", "#c200cc", "#8b00cc"],  # purples
            "nodata_val": 0,
            "nodata_color": "#ffffff00",
        },
    ],
    # "statistics": [ # for lake change dataset:
    #   {
    #     "name": "change_rate",
    #     "weight_by": "area",
    #     "property": "ChangeRateNet_myr-1",
    #     "aggregation_method": "min",
    #     "resampling_method": "mode",
    #     "val_range": [
    #       -2,
    #       2
    #     ],
    #     "palette": ["#ff0000", # red
    #                 "#FF8C00", # DarkOrange
    #                 "#FFA07A", # LightSalmon
    #                 "#FFFF00", # yellow
    #                 "#66CDAA", # MediumAquaMarine
    #                 "#AFEEEE", # PaleTurquoise,
    #                 "#0000ff"], # blue
    #     "nodata_val": 0,
    #     "nodata_color": "#ffffff00" # fully transparent white
    #   },
    # ],
    # "deduplicate_at": ["staging"],
    # "deduplicate_keep_rules": [["Perimeter_meter","larger"]], # [property, operator], using property with all positive values
    # "deduplicate_method": "neighbor",
    # "deduplicate_overlap_tolerance": 0.5, # default value
    # "deduplicate_overlap_both": False, # only 1 polygon must be overlapping with the deduplicate_overlap_tolerance threshold to be considered dups
    # "deduplicate_centroid_tolerance": None # only deduplicate_overlap_tolerance will be used to determine if polygons are dups
}
