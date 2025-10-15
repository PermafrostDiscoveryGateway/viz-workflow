# TifToWebTiles.py (rio-tiler version)
from typing import List, Optional, Sequence, Tuple, Iterable, Union
import numpy as np
import os
from pathlib import Path

import morecantile
from morecantile import Tile, TileMatrixSet
from rio_tiler.io import COGReader
from rio_tiler.errors import PointOutsideBounds

from pdgraster import WebImage, Palette
from colormaps.colormap import Colormap
import colormaps as cmaps


def _ensure_palette(
    colors: Optional[Union[Sequence[str], str]],
    nodata_color: Optional[str],
    reverse: bool = False,
) -> Palette:
    # hard defaults so callers may pass None
    if colors is None:
        colors = ["#ffffcc", "#c2e699", "#78c679", "#238443", "#004529"]
    if nodata_color is None:
        nodata_color = "#ffffff00"

    if isinstance(colors, str):
        cmap_palette = getattr(cmaps, colors)
        if reverse:
            cmap_palette = Colormap(cmap_palette.reversed().colors)
        return Palette(cmap_palette, nodata_color)
    cols = list(colors)[::-1] if reverse else list(colors)
    return Palette(cols, nodata_color)


def _band_names_from_reader(reader: COGReader) -> List[str]:
    n = reader.dataset.count
    names: List[str] = []
    try:
        for b in range(1, n + 1):
            tags = reader.dataset.tags(b)
            names.append(tags.get("NAME") or tags.get("BANDNAME") or f"band_{b}")
    except Exception:
        names = [f"band_{b}" for b in range(1, n + 1)]
    return names


def tiles_covering_bounds(
    tms: TileMatrixSet, bounds: Tuple[float, float, float, float], z: int
) -> Iterable[Tile]:
    w, s, e, n = bounds
    return tms.tiles(w, s, e, n, [z])


def generate_tiles_from_tif(
    tif_path: str,
    out_dir: str,
    *,
    tms_id: str = "WebMercatorQuad",   # set to your custom TMS id if needed
    min_z: int = 0,
    max_z: int = 7,
    colors: Optional[Union[Sequence[str], str]] = None,
    nodata_color: Optional[str] = None,
    reverse_palette: bool = False,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    style_prefix: Optional[str] = None,
    nodata_val_fallback: float = np.nan,
) -> List[str]:
    # Robust defaults and path check
    if colors is None:
        colors = ["#ffffcc", "#c2e699", "#78c679", "#238443", "#004529"]
    if nodata_color is None:
        nodata_color = "#ffffff00"
    tif_path = os.fspath(tif_path)
    if not Path(tif_path).is_file():
        raise FileNotFoundError(f"GeoTIFF not found: {Path(tif_path).resolve()}")

    os.makedirs(out_dir, exist_ok=True)

    tms = morecantile.tms.get(tms_id)
    palette = _ensure_palette(colors, nodata_color, reverse_palette)
    written: List[str] = []

    with COGReader(tif_path, tms=tms) as reader:
        bounds = reader.bounds
        band_names = _band_names_from_reader(reader)
        band_count = reader.dataset.count
        ds_nodata = reader.dataset.nodata
        nodata_val = ds_nodata if ds_nodata is not None else nodata_val_fallback

        # Auto min/max if not provided
        if min_value is None or max_value is None:
            try:
                stats = reader.statistics()
                mins = [s.min for s in stats.values()]
                maxs = [s.max for s in stats.values()]
                auto_min = float(np.nanmin(mins))
                auto_max = float(np.nanmax(maxs))
                min_value = auto_min if min_value is None else min_value
                max_value = auto_max if max_value is None else max_value
            except Exception:
                # conservative fallback
                min_value = -1.0 if min_value is None else min_value
                max_value =  1.0 if max_value is None else max_value

        for z in range(max_z, min_z - 1, -1):
            for tile in tiles_covering_bounds(tms, bounds, z):
                x, y = tile.x, tile.y
                try:
                    t = reader.tile(x, y, z)  # rio-tiler ImageData
                except PointOutsideBounds:
                    continue

                # ---- rio-tiler v6: handle array + mask (replaces .as_masked()) ----
                arr = t.array.astype("float32")          # (bands, H, W)
                mask = t.mask                             # (H, W) or (bands, H, W) or None
                if mask is not None:
                    if mask.ndim == 2:
                        mask = np.broadcast_to(mask, arr.shape)
                    arr = np.where(mask, np.nan, arr)
                # -------------------------------------------------------------------

                for b in range(band_count):
                    band_img = arr[b].copy()

                    # Respect dataset nodata
                    if ds_nodata is not None:
                        band_img[band_img == ds_nodata] = np.nan

                    # Clip to [min_value, max_value] with transparency outside
                    if min_value is not None:
                        band_img[band_img < min_value] = np.nan
                    if max_value is not None:
                        band_img[band_img > max_value] = np.nan

                    masked = np.ma.masked_invalid(band_img)
                    webimg = WebImage(
                        image_data=masked.filled(nodata_val),
                        palette=palette,
                        min_val=min_value,
                        max_val=max_value,
                        nodata_val=nodata_val,
                    )

                    style = style_prefix or (band_names[b] if b < len(band_names) else f"band_{b+1}")
                    rel = f"{style}/{tms_id}/{z}/{x}/{y}.png"
                    dst = os.path.join(out_dir, rel)
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    webimg.save(dst)
                    written.append(dst)

    return written
