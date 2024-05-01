from datetime import datetime
import subprocess
import numpy as np

# always include the tailing slash "/"
# define user on Delta, avoid writing files to other user's dir
user = subprocess.check_output("whoami").strip().decode("ascii")
head_node = 'cn014/'
#head_node = 'gpub___'

INPUT = '/scratch/bbou/julietcohen/infrastructure/input/20240423/'
output_subdir = 'infrastructure/output/20240423'
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
    13
  ],
  "geometricError": 57,
  "z_coord": 0,
  "statistics": [
    {
      "name": "palette_code",
      "weight_by": "area", 
      "property": "palette_code",
      "aggregation_method": "max", 
      "resampling_method": "nearest",
      "val_range": [
        1,
        7
      ], 
      "palette": [
        "#e67428", # 11=linear transport infrastructure (asphalt)
        "#9939a7", # 12=linear transport infrastructure (gravel)
        "#4daf4a", # 13=linear transport infrastructure (undefined)
        "#e41a1c", # 20=buildings (and other constructions such as bridges)
        "#787878", # 30=other impacted area (includes gravel pads, mining sites)
        "#e567e9", # 40=airstrip
        "#1f78b4"  # 50=reservoir or other water body impacted by human activities
      ],
      "nodata_val": 0,
      "nodata_color": "#ffffff00"
    },
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
        "#e67428", # 11=linear transport infrastructure (asphalt)
        "#9939a7", # 12=linear transport infrastructure (gravel)
        "#4daf4a", # 13=linear transport infrastructure (undefined)
        "#e41a1c", # 20=buildings (and other constructions such as bridges)
        "#787878", # 30=other impacted area (includes gravel pads, mining sites)
        "#e567e9", # 40=airstrip
        "#1f78b4"  # 50=reservoir or other water body impacted by human activities
      ],
      "nodata_val": 0,
      "nodata_color": "#ffffff00"
    }
  ]
}
