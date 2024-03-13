from datetime import datetime
import subprocess
import numpy as np

# always include the tailing slash "/"
# define user on Delta, avoid writing files to other user's dir
user = subprocess.check_output("whoami").strip().decode("ascii")
head_node = 'cn102/'
#head_node = 'gpub___'

INPUT = '/scratch/bbou/julietcohen/infrastructure/input/'
output_subdir = 'infrastructure/output'
OUTPUT  = f'/scratch/bbou/{user}/{output_subdir}/'

STAGING_LOCAL = '/tmp/staged/'
STAGING_REMOTE = OUTPUT  + 'staged/'
STAGING_REMOTE_MERGED = STAGING_REMOTE + head_node

GEOTIFF_LOCAL = '/tmp/geotiff/'
GEOTIFF_REMOTE = OUTPUT + 'geotiff/' 

WEBTILE_REMOTE = OUTPUT + 'web_tiles/'

""" final config is exported here, and imported in the workflow python file. """
CONFIG = {
  "deduplicate_clip_to_footprint": False,
  "deduplicate_method": None,
  "deduplicate_at": None,
  "deduplicate_keep_rules": None,
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
  "z_range": [
    0,
    12
  ],
  "geometricError": 57,
  "z_coord": 0,
  "statistics": [
    {
      "name": "infrastructure_code",
      "weight_by": "area", 
      "property": "DN",
      "aggregation_method": "max", 
      "resampling_method": "nearest",
      "val_range": [
        11,
        50
      ], 
      "palette": [
        "#f48525", 
        "#f4e625", 
        "#47f425", 
        "#25f4e2", 
        "#2525f4", 
        "#f425c3", 
        "#f42525" 
      ],
      "nodata_val": 0,
      "nodata_color": "#ffffff00"
    }
  ]
}
