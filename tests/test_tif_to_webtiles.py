import logging

import morecantile
import numpy as np
import pytest
import rasterio
from PIL import Image
from rasterio.transform import from_bounds

from pdgraster import Palette
from pdgworkflow import RasterTiler, WorkflowManager
from pdgworkflow.TifToWebTiles import BandStyle, generate_tiles_from_tif

NODATA = -9999.0
GRAY = Palette(["#000000", "#ffffff"], "#ffffff00")


def write_tif(path, data, bounds, crs="EPSG:4326", nodata=NODATA):
    data = np.asarray(data, dtype="float32")
    if data.ndim == 2:
        data = data[np.newaxis]
    count, height, width = data.shape
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=count,
        dtype="float32",
        crs=crs,
        transform=from_bounds(*bounds, width, height),
        nodata=nodata,
    ) as dst:
        dst.write(data)
    return str(path)


def path_manager_for(tmp_path, tms_id):
    def path_for(tile, kind, style=None):
        parts = [tms_id, str(tile.z), str(tile.x)]
        if kind == "web_tiles":
            return str(tmp_path.joinpath("web_tiles", style, *parts, f"{tile.y}.png"))
        return str(tmp_path.joinpath("geotiff", *parts, f"{tile.y}.tif"))

    return path_for


def rgba(path, row, col):
    return Image.open(path).convert("RGBA").getpixel((col, row))


def gray(value, lo, hi):
    """The color WebImage gives value with the GRAY palette and range."""
    return tuple(int(c) for c in GRAY.rgba_list[int((value - lo) * 255 / (hi - lo))])


def zxy(path):
    z, x, y = path.rsplit(".", 1)[0].split("/")[-3:]
    return int(z), int(x), int(y)


def quadrants(size=64):
    """NW=0, NE=nodata, SW=2, SE=3"""
    h = size // 2
    data = np.empty((size, size), dtype="float32")
    data[:h, :h] = 0
    data[:h, h:] = NODATA
    data[h:, :h] = 2
    data[h:, h:] = 3
    return data


@pytest.mark.parametrize(
    "tms_id, parent",
    [("WGS1984Quad", (1, 0, 2)), ("WebMercatorQuad", (1, 1, 2))],
)
def test_tiles_align_with_tms_and_nodata_is_transparent(tmp_path, tms_id, parent):
    # Raster exactly covers one z2 tile, so each z3 child is one quadrant.
    tms = morecantile.tms.get(tms_id)
    parent = morecantile.Tile(*parent)
    tif = write_tif(
        tmp_path / "in.tif", quadrants(), tms.xy_bounds(parent), crs=tms.rasterio_crs
    )
    path_for = path_manager_for(tmp_path, tms_id)

    written = generate_tiles_from_tif(
        [tif],
        path_for,
        tms=tms,
        min_z=2,
        max_z=3,
        styles=[BandStyle("v", GRAY, value_range=(0, 3))],
    )

    x, y = parent.x * 2, parent.y * 2
    nw, ne = morecantile.Tile(x, y, 3), morecantile.Tile(x + 1, y, 3)
    sw, se = morecantile.Tile(x, y + 1, 3), morecantile.Tile(x + 1, y + 1, 3)
    expected = {path_for(t, "web_tiles", "v") for t in (parent, nw, sw, se)}
    assert set(written) == expected  # NE is all nodata -> no tile

    assert rgba(path_for(nw, "web_tiles", "v"), 128, 128) == gray(0, 0, 3)
    assert rgba(path_for(sw, "web_tiles", "v"), 128, 128) == gray(2, 0, 3)
    assert rgba(path_for(se, "web_tiles", "v"), 128, 128) == gray(3, 0, 3)

    parent_png = path_for(parent, "web_tiles", "v")
    assert Image.open(parent_png).size == (256, 256)
    assert rgba(parent_png, 64, 192)[3] == 0  # NE quadrant transparent
    assert rgba(parent_png, 64, 64) == gray(0, 0, 3)
    assert rgba(parent_png, 192, 192) == gray(3, 0, 3)


def test_disjoint_inputs_leave_gap_empty(tmp_path):
    tms = morecantile.tms.get("WGS1984Quad")
    a = write_tif(tmp_path / "a.tif", np.ones((16, 16)), (-170, 0, -160, 10))
    b = write_tif(tmp_path / "b.tif", np.ones((16, 16)) * 2, (160, 0, 170, 10))
    path_for = path_manager_for(tmp_path, "WGS1984Quad")

    written = generate_tiles_from_tif(
        [a, b], path_for, tms=tms, min_z=0, max_z=3, styles=[BandStyle("v", GRAY)]
    )

    xs = {x for z, x, _ in map(zxy, written) if z == 3}
    # z3 tiles are 22.5 deg wide: only the outermost columns have data
    assert xs == {0, 15}


def test_projected_input_writes_tiles_in_tms_crs(tmp_path):
    tms = morecantile.tms.get("WGS1984Quad")
    # A patch of northern Alaska/Canada in polar stereographic
    tif = write_tif(
        tmp_path / "polar.tif",
        np.arange(32 * 32, dtype="float32").reshape(32, 32),
        (-2_400_000, -500_000, -2_000_000, -100_000),
        crs="EPSG:3413",
    )
    path_for = path_manager_for(tmp_path, "WGS1984Quad")

    written = generate_tiles_from_tif(
        [tif], path_for, tms=tms, min_z=0, max_z=4, styles=[BandStyle("v", GRAY)]
    )

    assert written
    tile_tifs = sorted((tmp_path / "geotiff").rglob("*.tif"))
    assert tile_tifs
    with rasterio.open(tile_tifs[0]) as src:
        assert src.crs.to_epsg() == 4326


def test_colors_use_one_stretch_across_tiles(tmp_path):
    # Raster straddles the z1 tile boundary at lon -90. Left half spans
    # 0..50 and right half 50..100, so a per-tile stretch would render 50
    # differently on each side.
    data = np.full((16, 32), 50, dtype="float32")
    data[:, 0] = 0
    data[:, -1] = 100
    tms = morecantile.tms.get("WGS1984Quad")
    tif = write_tif(tmp_path / "in.tif", data, (-100, 0, -80, 10))
    path_for = path_manager_for(tmp_path, "WGS1984Quad")

    generate_tiles_from_tif(
        [tif], path_for, tms=tms, min_z=1, max_z=1, styles=[BandStyle("v", GRAY)]
    )

    colors = set()
    for x in (0, 1):
        tile = morecantile.Tile(x, 0, 1)
        with rasterio.open(path_for(tile, "geotiff")) as src:
            values = src.read(1)
        img = np.asarray(Image.open(path_for(tile, "web_tiles", "v")).convert("RGBA"))
        colors |= {tuple(c) for c in img[values == 50]}
    assert colors == {gray(50, 0, 100)}


def test_configured_value_range_is_used(tmp_path):
    tms = morecantile.tms.get("WGS1984Quad")
    tif = write_tif(tmp_path / "in.tif", np.full((8, 8), 50), (-100, 0, -95, 5))
    path_for = path_manager_for(tmp_path, "WGS1984Quad")

    [png] = generate_tiles_from_tif(
        [tif],
        path_for,
        tms=tms,
        min_z=0,
        max_z=0,
        styles=[BandStyle("v", GRAY, value_range=lambda z: (0, 200))],
    )

    img = np.asarray(Image.open(png).convert("RGBA"))
    assert {tuple(int(v) for v in c) for c in img[img[..., 3] > 0]} == {
        gray(50, 0, 200)
    }


def test_multiband_renders_one_style_per_configured_stat(tmp_path, caplog):
    tms = morecantile.tms.get("WGS1984Quad")
    data = np.stack([np.full((8, 8), v, dtype="float32") for v in (1, 2, 3)])
    tif = write_tif(tmp_path / "in.tif", data, (-100, 0, -95, 5))
    path_for = path_manager_for(tmp_path, "WGS1984Quad")

    with caplog.at_level(logging.WARNING):
        generate_tiles_from_tif(
            [tif],
            path_for,
            tms=tms,
            min_z=0,
            max_z=1,
            styles=[BandStyle("a", GRAY), BandStyle("b", GRAY)],
        )

    assert sorted(p.name for p in (tmp_path / "web_tiles").iterdir()) == ["a", "b"]
    assert "3 band(s) but 2 style(s)" in caplog.text
    with rasterio.open(next((tmp_path / "geotiff").rglob("*.tif"))) as src:
        assert src.count == 3


def test_overwrite_false_skips_existing_tiles(tmp_path):
    tms = morecantile.tms.get("WGS1984Quad")
    tif = write_tif(tmp_path / "in.tif", np.ones((8, 8)), (-100, 0, -95, 5))
    path_for = path_manager_for(tmp_path, "WGS1984Quad")
    kwargs = dict(tms=tms, min_z=0, max_z=1, styles=[BandStyle("v", GRAY)])

    assert generate_tiles_from_tif([tif], path_for, **kwargs)
    assert generate_tiles_from_tif([tif], path_for, overwrite=False, **kwargs) == []


def test_raster_tiler_finds_tif_and_tiff_inputs(tmp_path):
    write_tif(tmp_path / "in" / "a.tif", np.ones((4, 4)), (0, 0, 1, 1))
    write_tif(tmp_path / "in" / "sub" / "b.TIFF", np.ones((4, 4)), (1, 0, 2, 1))
    (tmp_path / "in" / "notes.txt").write_text("x")
    tiler = RasterTiler({"dir_input": str(tmp_path / "in")})  # ext_input=.shp

    found = tiler._find_input_geotiffs()

    assert [p.rsplit("/", 1)[-1] for p in found] == ["a.tif", "b.TIFF"]


def test_run_workflow_tiles_geotiffs_and_writes_capabilities(tmp_path):
    data = np.tile(np.linspace(0, 100, 32, dtype="float32"), (32, 1))
    write_tif(tmp_path / "input" / "a.tif", data, (-150, 60, -140, 70))
    manager = WorkflowManager(
        {
            "dir_input": str(tmp_path / "input"),
            "ext_input": ".tif",
            "dir_web_tiles": str(tmp_path / "web_tiles"),
            "dir_geotiff": str(tmp_path / "geotiff"),
            "enable_geotiff_tiling": True,
            "enable_stager": False,
            "enable_raster": False,
            "enable_3dtiles": False,
            "z_range": (0, 3),
            "statistics": [
                {
                    "name": "value",
                    "val_range": [0, 100],
                    "palette": ["#0000ff", "#ff0000"],
                    "nodata_val": None,
                }
            ],
        }
    )

    manager.run_workflow()

    pngs = list((tmp_path / "web_tiles" / "value" / "WGS1984Quad").rglob("*.png"))
    assert {int(p.parent.parent.name) for p in pngs} == {0, 1, 2, 3}
    assert list((tmp_path / "geotiff" / "WGS1984Quad").rglob("*.tif"))
    capabilities = (tmp_path / "input" / "WMTSCapabilities.xml").read_text()
    assert "value" in capabilities
