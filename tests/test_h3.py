from types import SimpleNamespace

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

from pdgworkflow import WorkflowManager


def test_run_h3_staging_passes_every_h3_option_to_the_stager(tmp_path):
    calls = []
    manager = WorkflowManager(
        {
            "dir_h3": str(tmp_path / "h3"),
            "h3_res": [2, 3],
            "h3_intersect_cells": True,
            "h3_dissolve_overlaps": True,
            "h3_attr_to_sum": ["val"],
            "h3_attr_to_mean": ["val"],
            "h3_area_epsg": "6933",
            "h3_3dtiles": {"enabled": False},
        }
    )
    manager.h3_stager = SimpleNamespace(stage_all=lambda **kwargs: calls.append(kwargs))

    assert manager.run_h3_staging() is True

    assert calls == [
        {
            "h3_res": [2, 3],
            "h3_intersect_cells": True,
            "dissolve_overlaps": True,
            "attr_to_sum": ["val"],
            "attr_to_mean": ["val"],
            "land_polygons_path": None,
            "area_epsg": "6933",
        }
    ]


def test_run_h3_staging_summarizes_a_real_input(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "input").mkdir()
    squares = [box(-150 + i * 0.3, 65 + j * 0.3, -149.8 + i * 0.3, 65.2 + j * 0.3)
               for i in range(6) for j in range(6)]
    gpd.GeoDataFrame(
        {"val": [float(n) for n in range(36)]}, geometry=squares, crs="EPSG:4326"
    ).to_file(tmp_path / "input" / "a.gpkg")

    WorkflowManager(
        {
            "ext_input": ".gpkg",
            "enable_stager": False,
            "h3_res": [2, 3],
            "h3_attr_to_sum": ["val"],
            "h3_attr_to_mean": ["val"],
            "h3_3dtiles": {"enabled": False},
        }
    ).run_h3_staging()

    for resolution in (2, 3):
        summary = gpd.read_file(tmp_path / "h3" / f"summary_h3r{resolution}.gpkg")
        assert summary["_count"].sum() == 36
        assert summary["sum_val"].sum() == sum(range(36))
    assert pd.read_csv(tmp_path / "h3" / "h3_summary.csv")["ok"].all()
