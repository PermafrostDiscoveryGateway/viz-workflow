from pdgworkflow import ConfigManager, StagedTo3DConverter, WorkflowManager


def test_defaults_are_valid_without_a_config_file():
    config = ConfigManager()

    assert config.get("tms_id") == "WGS1984Quad"
    assert WorkflowManager().config.get("dir_input") == "input"


def test_staging_options_come_straight_from_config():
    config = ConfigManager(
        {"tms_id": "WebMercatorQuad", "z_range": (2, 5), "input_crs": "EPSG:4326"}
    )

    assert config.get("tms_id") == "WebMercatorQuad"
    assert config.get_max_z() == 5
    assert list(config.get("tile_path_structure")) == ["style", "tms", "z", "x", "y"]
    assert config.get("input_crs") == "EPSG:4326"
    assert config.get("simplify_tolerance") == 0.0001


def test_raster_config_uses_existing_raster_configuration():
    raster = ConfigManager({"tile_size": (64, 32)}).get_raster_config()

    assert raster["shape"] == (64, 32)
    assert raster["centroid_properties"] == (
        "staging_centroid_x",
        "staging_centroid_y",
    )
    assert raster["stats"][0]["name"] == "polygon_count"


def test_3d_leaf_z_uses_existing_tiles_configuration():
    assert (
        StagedTo3DConverter(
            ConfigManager({"z_coord": 12, "geometricError": 3, "version": "v1"})
        ).get_3dtiles_z_coord()
        == 12
    )
    assert (
        StagedTo3DConverter(ConfigManager({"z_coord": 0})).get_3dtiles_z_coord()
        == ConfigManager({}).get_max_z()
    )
