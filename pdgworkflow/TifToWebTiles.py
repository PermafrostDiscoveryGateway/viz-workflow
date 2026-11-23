"""
Create web tiles (PNG) and per-tile GeoTIFFs directly from existing
GeoTIFFs, for use with web maps such as Cesium.

The input rasters are read with rio-tiler in the target TileMatrixSet and
mosaicked tile by tile, so inputs may be in any CRS and may overlap or
leave gaps between each other. Colors are stretched using one value range
per band (and z-level), not per tile, so adjacent tiles match.
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Sequence, Tuple, Union

import numpy as np
import rasterio
from morecantile import Tile, TileMatrixSet
from rasterio.enums import Resampling
from rasterio.transform import from_bounds
from rasterio.warp import transform_bounds
from rio_tiler.errors import EmptyMosaicError, PointOutsideBounds, TileOutsideBounds
from rio_tiler.io import Reader
from rio_tiler.mosaic import mosaic_reader
from rio_tiler.mosaic.methods import FirstMethod
from rio_tiler.mosaic.methods.base import MosaicMethodBase

from pdgraster import Palette, WebImage

logger = logging.getLogger(__name__)

TIF_EXTENSIONS = (".tif", ".tiff")
DEFAULT_COLORS = ["#c2e699", "#78c679", "#238443", "#004529"]
# transparent
DEFAULT_NODATA_COLOR = "#ffffff00"
# Value used to mark masked pixels for WebImage when a band has no
# configured nodata value. WebImage can't detect NaN as nodata.
NODATA_SENTINEL = float(np.finfo(np.float32).min)
# Max dimension of the decimated read used to compute value ranges
RANGE_SAMPLE_SIZE = 1024

ValueRange = Tuple[Optional[float], Optional[float]]
# Returns the output path for a tile; kind is "web_tiles" or "geotiff"
PathFor = Callable[[Tile, str, Optional[str]], str]


@dataclass
class BandStyle:
    """
    How to render one band of the input GeoTIFFs as web tiles.

    Attributes
    ----------
    name : str
        Style name; used in the output path of the web tiles.
    palette : pdgraster.Palette
        Colors used to render the band.
    value_range : tuple or callable
        (min, max) values mapped to the first and last palette colors, or
        a function of the z-level that returns one. Either end may be
        None, in which case the min/max of the band across all inputs is
        used.
    nodata_val : float or None
        Pixels with this value are also rendered with the nodata color
        (in addition to pixels masked in the source GeoTIFFs).
    """

    name: str
    palette: Palette = field(
        default_factory=lambda: Palette(DEFAULT_COLORS, DEFAULT_NODATA_COLOR)
    )
    value_range: Union[ValueRange, Callable[[int], ValueRange]] = (None, None)
    nodata_val: Optional[float] = None

    def range_for_z(self, z: int) -> ValueRange:
        vr = self.value_range(z) if callable(self.value_range) else self.value_range
        return tuple(vr) if vr is not None else (None, None)


def _band_names(dataset) -> List[str]:
    """Band names from GeoTIFF tags/descriptions, falling back to band_<i>."""
    names = []
    for b in range(1, dataset.count + 1):
        tags = dataset.tags(b)
        desc = dataset.descriptions[b - 1]
        names.append(tags.get("NAME") or tags.get("BANDNAME") or desc or f"band_{b}")
    return names


def compute_global_range(
    tif_paths: Sequence[str], bidx: int, exclude: Optional[float] = None
) -> ValueRange:
    """
    Approximate min and max of one band across several GeoTIFFs.

    Each file is read decimated (largest side at most RANGE_SAMPLE_SIZE
    pixels) with its nodata/mask applied.

    Parameters
    ----------
    tif_paths : list of str
        The GeoTIFFs.
    bidx : int
        1-based band index.
    exclude : float, optional
        A value to ignore (e.g. a configured nodata value).

    Returns
    -------
    tuple
        (min, max), or (None, None) if the band has no valid pixels.
    """
    mins, maxs = [], []
    for path in tif_paths:
        with rasterio.open(path) as src:
            scale = max(1.0, max(src.width, src.height) / RANGE_SAMPLE_SIZE)
            shape = (max(1, int(src.height / scale)), max(1, int(src.width / scale)))
            data = src.read(
                bidx, out_shape=shape, masked=True, resampling=Resampling.nearest
            ).astype("float64")
        data = np.ma.masked_invalid(data)
        if exclude is not None:
            data = np.ma.masked_equal(data, exclude)
        if data.count():
            mins.append(float(data.min()))
            maxs.append(float(data.max()))
    if not mins:
        return (None, None)
    return (min(mins), max(maxs))


def _bounds_intersect(a, b) -> bool:
    return a[0] < b[2] and a[2] > b[0] and a[1] < b[3] and a[3] > b[1]


def generate_tiles_from_tif(
    tif_paths: Union[str, Sequence[str]],
    path_for: PathFor,
    *,
    tms: TileMatrixSet,
    min_z: int = 0,
    max_z: int = 13,
    styles: Optional[Sequence[BandStyle]] = None,
    pixel_selection: Union[type, MosaicMethodBase] = FirstMethod,
    tilesize: int = 256,
    save_tiff: bool = True,
    overwrite: bool = True,
) -> List[str]:
    """
    Create web tiles from GeoTIFFs for every tile of ``tms`` that the
    inputs cover, from ``max_z`` down to ``min_z``.

    Parameters
    ----------
    tif_paths : str or list of str
        GeoTIFF(s) to tile. All must have a CRS and the same band count.
    path_for : callable
        ``path_for(tile, kind, style)`` returns the output path for a
        tile, where kind is "web_tiles" (with a style name) or "geotiff"
        (style is None).
    tms : morecantile.TileMatrixSet
        The tiling scheme of the output.
    min_z, max_z : int
        The range of z-levels to create.
    styles : list of BandStyle, optional
        How to render each band; band i uses styles[i]. Bands beyond the
        number of styles are not rendered. If None, every band is rendered
        with the default palette, named from its band tags.
    pixel_selection : MosaicMethod
        How to choose a pixel where inputs overlap. Defaults to the first
        input (in order) that has a valid pixel.
    tilesize : int
        Width and height of output tiles in pixels.
    save_tiff : bool
        Also write a float32 GeoTIFF per tile (all bands, in the TMS CRS).
    overwrite : bool
        If False, skip tiles whose outputs already exist.

    Returns
    -------
    list of str
        Paths of the web tiles that were written.
    """
    tif_list = [str(tif_paths)] if isinstance(tif_paths, str) else [str(p) for p in tif_paths]
    if not tif_list:
        raise ValueError("No GeoTIFFs passed to generate_tiles_from_tif")

    asset_bounds = {}
    band_count = None
    src_nodata = None
    band_names: List[str] = []
    for path in tif_list:
        with Reader(path, tms=tms) as r:
            if r.crs is None:
                raise ValueError(f"GeoTIFF has no CRS: {path}")
            if band_count is None:
                band_count = r.dataset.count
                src_nodata = r.dataset.nodata
                band_names = _band_names(r.dataset)
            elif r.dataset.count != band_count:
                raise ValueError(
                    f"All GeoTIFFs must have the same number of bands; {path} has "
                    f"{r.dataset.count}, expected {band_count}"
                )
            # Bounds in the TMS's geographic CRS, as tms.tiles() expects
            asset_bounds[path] = transform_bounds(
                r.crs, tms.rasterio_geographic_crs, *r.bounds, densify_pts=21
            )

    if styles is None:
        styles = [BandStyle(name=n) for n in band_names]
    if len(styles) != band_count:
        logger.warning(
            f"Inputs have {band_count} band(s) but {len(styles)} style(s) were "
            f"given; rendering the first {min(band_count, len(styles))}."
        )
    styles = list(styles)[:band_count]

    # Fill unset ends of each style's value range from the data itself so
    # every tile uses the same stretch.
    data_ranges = {}

    def range_for(i: int, z: int) -> ValueRange:
        lo, hi = styles[i].range_for_z(z)
        if lo is None or hi is None:
            if i not in data_ranges:
                data_ranges[i] = compute_global_range(
                    tif_list, i + 1, exclude=styles[i].nodata_val
                )
            dlo, dhi = data_ranges[i]
            lo = dlo if lo is None else lo
            hi = dhi if hi is None else hi
        return lo, hi

    w = min(b[0] for b in asset_bounds.values())
    s = min(b[1] for b in asset_bounds.values())
    e = max(b[2] for b in asset_bounds.values())
    n = max(b[3] for b in asset_bounds.values())

    def _tile_reader(asset: str, x: int, y: int, z: int, **kwargs):
        with Reader(asset, tms=tms) as src:
            return src.tile(x, y, z, tilesize=tilesize, **kwargs)

    tiff_nodata = src_nodata if src_nodata is not None else np.nan
    written: List[str] = []

    for z in range(max_z, min_z - 1, -1):
        for tile in tms.tiles(w, s, e, n, [z]):
            web_paths = [path_for(tile, "web_tiles", st.name) for st in styles]
            tif_out = path_for(tile, "geotiff", None) if save_tiff else None
            if not overwrite and all(
                os.path.isfile(p) for p in web_paths + ([tif_out] if tif_out else [])
            ):
                continue

            tile_bounds = tms.bounds(tile)
            assets = [a for a in tif_list if _bounds_intersect(asset_bounds[a], tile_bounds)]
            if not assets:
                continue

            try:
                img, _ = mosaic_reader(
                    assets,
                    _tile_reader,
                    tile.x,
                    tile.y,
                    tile.z,
                    pixel_selection=pixel_selection,
                )
            except (TileOutsideBounds, PointOutsideBounds, EmptyMosaicError):
                continue

            arr = np.ma.masked_invalid(img.array.astype("float32"))
            invalid = np.ma.getmaskarray(arr)
            if invalid.all():
                continue

            if tif_out:
                _write_tile_tiff(tif_out, arr, invalid, tms, tile, tiff_nodata)

            for i, style in enumerate(styles):
                band_invalid = invalid[i]
                if style.nodata_val is not None:
                    band_invalid = band_invalid | (arr.data[i] == style.nodata_val)
                if band_invalid.all():
                    continue
                fill = style.nodata_val if style.nodata_val is not None else NODATA_SENTINEL
                band = np.where(band_invalid, fill, arr.data[i]).astype("float64")
                lo, hi = range_for(i, z)
                WebImage(
                    image_data=band,
                    palette=style.palette,
                    min_val=lo,
                    max_val=hi,
                    nodata_val=fill,
                ).save(web_paths[i])
                written.append(web_paths[i])

    return written


def _write_tile_tiff(path, arr, invalid, tms, tile, nodata):
    """Write all bands of one tile as a float32 GeoTIFF in the TMS CRS."""
    left, bottom, right, top = tms.xy_bounds(tile)
    count, height, width = arr.shape
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    profile = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": count,
        "dtype": "float32",
        "crs": tms.rasterio_crs,
        "transform": from_bounds(left, bottom, right, top, width, height),
        "nodata": nodata,
    }
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(np.where(invalid, nodata, arr.data).astype("float32"))
