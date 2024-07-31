workflow_config = {
    #"deduplicate_method": "footprints",
    "deduplicate_method": None,
    #"deduplicate_clip_to_footprint": True,
    "deduplicate_clip_to_footprint": False,
    #"deduplicate_at": ["raster"],
    "deduplicate_at": None,
    #"deduplicate_keep_rules": [["Date","larger"]],
    "deduplicate_keep_rules": None,
    "dir_input": "/data/viz_workflow/input/",
    #"dir_footprints": "/data/viz_workflow/input_3_iwp_fp/footprints/",
    #"ext_input": ".shp",
    "ext_input": ".gpkg",
    #"ext_footprints": ".shp",
    "dir_staged": "/data/viz_workflow/output/test_0718/staged/",
    "dir_geotiff": "/data/viz_workflow/output/test_0718/geotiff/",
    "dir_web_tiles": "/data/viz_workflow/output/test_0718/web_tiles/",
    "filename_staging_summary": "/data/viz_workflow/output/test_0718/staging_summary.csv",
    "filename_rasterization_events": "/data/viz_workflow/output/test_0718/raster_events.csv",
    "filename_rasters_summary": "/data/viz_workflow/output/test_0718/raster_summary.csv",
    "simplify_tolerance": 0.1,
    "tms_id": "WGS1984Quad",
    "z_range": [0,15],
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
