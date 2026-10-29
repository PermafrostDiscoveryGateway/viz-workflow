import pdg3dtiles
import pdgraster
import pdgstaging
import pytest

from pdgworkflow import ConfigManager, RasterTiler, StagedTo3DConverter, WorkflowManager


def test_lower_package_dependencies_are_immutable_refs():
    pyproject = open("pyproject.toml").read()
    assert "@main" not in pyproject
    assert pyproject.count("git+https://github.com/PermafrostDiscoveryGateway/") == 3


def test_raster_tiler_delegates_one_explicit_tile(monkeypatch):
    calls = []

    def rasterize_tile(*args, **kwargs):
        calls.append((args, kwargs))
        return "result"

    monkeypatch.setattr(pdgraster, "rasterize_tile", rasterize_tile, raising=False)
    result = RasterTiler({"tile_size": (32, 32)}).rasterize_tile(
        "tile.gpkg", "tile.tif", bounds={"left": 0}, overwrite=True
    )

    assert result == "result"
    assert calls == [
        (
            ("tile.gpkg", "tile.tif"),
            {
                "bounds": {"left": 0},
                "shape": (32, 32),
                "centroid_properties": ("staging_centroid_x", "staging_centroid_y"),
                "statistics": [
                    {
                        "name": "polygon_count",
                        "weight_by": "count",
                        "property": "centroids_per_pixel",
                        "aggregation_method": "sum",
                    },
                    {
                        "name": "coverage",
                        "weight_by": "area",
                        "property": "area_per_pixel_area",
                        "aggregation_method": "sum",
                    },
                ],
                "overwrite": True,
            },
        )
    ]


def test_raster_tiler_tile_propagates_errors(monkeypatch):
    def rasterize_tile(*args, **kwargs):
        raise RuntimeError("rasterize failed")

    monkeypatch.setattr(pdgraster, "rasterize_tile", rasterize_tile, raising=False)

    with pytest.raises(RuntimeError, match="rasterize failed"):
        RasterTiler({"tile_size": (32, 32)}).rasterize_tile(
            "tile.gpkg", "tile.tif", bounds={"left": 0}
        )


def test_3d_converter_delegates_one_explicit_leaf(monkeypatch):
    calls = []
    monkeypatch.setattr(
        pdg3dtiles,
        "convert_leaf",
        lambda *args, **kwargs: calls.append((args, kwargs)) or "result",
        raising=False,
    )

    result = StagedTo3DConverter(ConfigManager({"z_coord": 12})).convert_leaf(
        "tile.gpkg", "tile.b3dm", "tile.json"
    )

    assert result == "result"
    assert calls == [
        (
            ("tile.gpkg", "tile.b3dm", "tile.json"),
            {"z": 12, "geometric_error": None, "version": None},
        )
    ]


def test_3d_converter_leaf_propagates_errors(monkeypatch):
    def convert_leaf(*args, **kwargs):
        raise RuntimeError("convert failed")

    monkeypatch.setattr(pdg3dtiles, "convert_leaf", convert_leaf, raising=False)

    with pytest.raises(RuntimeError, match="convert failed"):
        StagedTo3DConverter(ConfigManager({"z_coord": 12})).convert_leaf(
            "tile.gpkg", "tile.b3dm", "tile.json"
        )


def test_3d_converter_leaf_falls_back_to_max_z(monkeypatch):
    calls = []
    monkeypatch.setattr(
        pdg3dtiles,
        "convert_leaf",
        lambda *args, **kwargs: calls.append((args, kwargs)) or "result",
        raising=False,
    )

    StagedTo3DConverter(ConfigManager({"z_coord": 0})).convert_leaf(
        "tile.gpkg", "tile.b3dm", "tile.json"
    )

    assert calls[0][1]["z"] == ConfigManager({}).get_max_z()


def test_workflow_manager_delegates_one_explicit_source(monkeypatch):
    calls = []
    monkeypatch.setattr(
        pdgstaging,
        "stage_source",
        lambda *args, **kwargs: calls.append((args, kwargs)) or "result",
        raising=False,
    )

    result = WorkflowManager({"input_crs": "EPSG:4326"}).stage_source(
        "source.gpkg", "shards", "source-a", overwrite=True
    )

    assert result == "result"
    assert calls == [
        (
            ("source.gpkg", "shards", "source-a"),
            {
                "tms_id": "WGS1984Quad",
                "z": 13,
                "path_structure": ["style", "tms", "z", "x", "y"],
                "properties": WorkflowManager().props,
                "input_crs": "EPSG:4326",
                "tolerance": 0.0001,
                "overwrite": True,
            },
        )
    ]


def test_workflow_manager_stage_source_propagates_errors(monkeypatch):
    def stage_source(*args, **kwargs):
        raise RuntimeError("staging failed")

    monkeypatch.setattr(pdgstaging, "stage_source", stage_source, raising=False)

    with pytest.raises(RuntimeError, match="staging failed"):
        WorkflowManager({}).stage_source("source.gpkg", "shards", "source-a")
