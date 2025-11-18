from pathlib import Path
from typing import List, Optional, Sequence, Union

import numpy as np
import rasterio
import morecantile
from morecantile import TileMatrixSet
from rasterio.transform import from_bounds
from rio_tiler.io import COGReader
from rio_tiler.errors import PointOutsideBounds, TileOutsideBounds

from pdgraster import WebImage, Palette
from colormaps.colormap import Colormap
import colormaps as cmaps


DEFAULT_COLORS = ["#00000000", "#c2e699", "#78c679", "#238443", "#004529"]
DEFAULT_NODATA_COLOR = "#00000000"


def _ensure_palette(
    colors: Optional[Union[Sequence[str], str]],
    nodata_color: Optional[str],
    reverse: bool = False,
) -> Palette:
    colors = colors or DEFAULT_COLORS
    nodata_color = nodata_color or DEFAULT_NODATA_COLOR

    if isinstance(colors, str):
        cmap: Colormap = getattr(cmaps, colors)
        if reverse:
            cmap = Colormap(cmap.reversed().colors)
        return Palette(cmap, nodata_color)

    cols = list(colors)
    if reverse:
        cols.reverse()
    return Palette(cols, nodata_color)


def _band_names_from_reader(reader: COGReader) -> List[str]:
    count = reader.dataset.count
    names: List[str] = []

    try:
        for b in range(1, count + 1):
            tags = reader.dataset.tags(b)
            names.append(tags.get("NAME") or tags.get("BANDNAME") or f"band_{b}")
    except Exception:
        # Fallback if tags access fails for any band
        names = [f"band_{b}" for b in range(1, count + 1)]

    return names


def tiles_covering_bounds(
    tms: TileMatrixSet,
    bounds: Sequence[float],
    z: int,
):
    w, s, e, n = bounds
    return tms.tiles(w, s, e, n, [z])


def generate_tiles_from_tif(
    tif_path: Union[str, Path],
    out_dir: Union[str, Path],
    *,
    tms_id: str = "WGS1984Quad",
    min_z: int = 0,
    max_z: int = 7,
    colors: Optional[Union[Sequence[str], str]] = None,
    nodata_color: Optional[str] = None,
    reverse_palette: bool = False,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    style_prefix: Optional[str] = None,
    nodata_val_fallback: float = np.nan,
    save_tiff: bool = True,
) -> List[str]:

    tif_path_p = Path(tif_path)
    if not tif_path_p.is_file():
        raise FileNotFoundError(f"GeoTIFF not found: {tif_path_p.resolve()}")

    web_root = Path(out_dir)
    png_root = web_root / "png"
    tif_root = web_root / "tiff"

    png_root.mkdir(parents=True, exist_ok=True)
    if save_tiff:
        tif_root.mkdir(parents=True, exist_ok=True)

    tms = morecantile.tms.get(tms_id)
    palette = _ensure_palette(colors, nodata_color, reverse_palette)
    written: List[str] = []

    with COGReader(str(tif_path_p), tms=tms) as reader:
        if reader.crs is None:
            raise ValueError(f"{tif_path_p} has no CRS/georeferencing.")

        bounds = getattr(reader, "geographic_bounds", reader.bounds)

        band_names = _band_names_from_reader(reader)
        band_count = reader.dataset.count
        ds_nodata = reader.dataset.nodata
        nodata_val = ds_nodata if ds_nodata is not None else nodata_val_fallback

        for z in range(max_z, min_z - 1, -1):
            for tile in tiles_covering_bounds(tms, bounds, z):
                x, y = tile.x, tile.y

                try:
                    image = reader.tile(x, y, z)
                except (TileOutsideBounds, PointOutsideBounds):
                    continue

                arr = image.array.astype("float32", copy=False)
                mask = image.mask

                # Apply mask (0 = nodata)
                if mask is not None:
                    if mask.ndim == 2:
                        mask = np.broadcast_to(mask, arr.shape)
                    arr = np.where(mask == 0, np.nan, arr)

                # Apply dataset nodata once to all bands
                if ds_nodata is not None:
                    arr = np.where(arr == ds_nodata, np.nan, arr)

                # -------------------------------
                # 1. SAVE GEOTIFF TILE
                # -------------------------------
                if save_tiff:
                    left, bottom, right, top = tms.xy_bounds(tile)
                    _, height, width = arr.shape
                    transform = from_bounds(left, bottom, right, top, width, height)
                    arr_tif = np.where(np.isnan(arr), nodata_val, arr)

                    tif_rel = Path(tms_id) / str(z) / str(x) / f"{y}.tif"
                    tif_path_tile = tif_root / tif_rel
                    tif_path_tile.parent.mkdir(parents=True, exist_ok=True)

                    profile = {
                        "driver": "GTiff",
                        "height": height,
                        "width": width,
                        "count": band_count,
                        "dtype": "float32",
                        "crs": reader.crs,
                        "transform": transform,
                        "nodata": nodata_val,
                    }

                    with rasterio.open(tif_path_tile, "w", **profile) as dst:
                        dst.write(arr_tif)

                # -------------------------------
                # 2. SAVE PNG WEB TILES (per-band)
                # -------------------------------
                for b in range(band_count):
                    band = arr[b]

                    # Determine value range
                    if min_value is None or max_value is None:
                        if not np.isfinite(band).any():
                            continue
                        bmin = float(np.nanmin(band))
                        bmax = float(np.nanmax(band))
                        if bmin == bmax:
                            continue
                    else:
                        bmin, bmax = float(min_value), float(max_value)

                    masked = np.ma.masked_invalid(band)

                    web_img = WebImage(
                        image_data=masked.filled(nodata_val),
                        palette=palette,
                        min_val=bmin,
                        max_val=bmax,
                        nodata_val=nodata_val,
                    )

                    style = style_prefix or band_names[b]
                    png_rel = Path(style) / tms_id / str(z) / str(x) / f"{y}.png"
                    png_path = png_root / png_rel
                    png_path.parent.mkdir(parents=True, exist_ok=True)

                    web_img.save(str(png_path))
                    written.append(str(png_path))

    return written
