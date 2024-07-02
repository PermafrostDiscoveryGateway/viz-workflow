workflow_config = {
    "deduplicate_method": None,
    "deduplicate_clip_to_footprint": False,
    "dir_input": "/data/viz_workflow/input/",
    "ext_input": ".gpkg",
    "dir_staged": "/data/viz_workflow/output/staged/",
    "dir_geotiff": "/data/viz_workflow/output/geotiff/",
    "dir_web_tiles": "/data/viz_workflow/output/web_tiles/",
    "filename_staging_summary": "/data/viz_workflow/output/staging_summary.csv",
    "filename_rasterization_events": "/data/viz_workflow/output/raster_events.csv",
    "filename_rasters_summary": "/data/viz_workflow/output/raster_summary.csv",
    "simplify_tolerance": 0.1,
    "tms_id": "WGS1984Quad",
    "z_range": [
    0,
    9 # increase this later to 15
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
