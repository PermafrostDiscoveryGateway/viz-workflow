from types import SimpleNamespace

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
