workflow_config = {
    "deduplicate_clip_to_footprint": False,
    "deduplicate_method": None,
    "dir_output": ".", # output written to /usr/local/share/app
    "dir_input": "input", # this dir is inside the dir COPY'd into container (because docker doesn't copy the top dir itself)
    "ext_input": ".gpkg",
    "dir_staged": "/mnt/data/staged/", 
    "dir_geotiff": "/mnt/data/geotiff/",  
    "dir_web_tiles": "/mnt/data/web_tiles/", 
    "filename_staging_summary": "/mnt/data/staging_summary.csv",
    "filename_rasterization_events": "/mnt/data/raster_events.csv",
    "filename_rasters_summary": "/mnt/data/raster_summary.csv",
    "simplify_tolerance": 0.1,
    "tms_id": "WGS1984Quad",
    "z_range": [
    0,
    10 # increase this later to 15
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
          "#f8ff1f1A", # 10% alpha yellow
          "#f8ff1f" # solid yellow
      ], 
    "nodata_val": 0,
    "nodata_color": "#ffffff00"
    }
  ]
}