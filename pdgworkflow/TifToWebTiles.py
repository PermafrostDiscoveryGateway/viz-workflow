from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Union

import numpy as np
import morecantile
from morecantile import TileMatrixSet
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
        names = [f"band_{b}" for b in range(1, count + 1)]
    return names


def tiles_covering_bounds(
    tms: TileMatrixSet,
    bounds: Sequence[float],
    z: int,
) -> Iterable[morecantile.Tile]:
    w, s, e, n = bounds
    return tms.tiles(w, s, e, n, [z])


def generate_tiles_from_tif(
    tif_path: str,
    out_dir: str,
    *,
    tms_id: str = "WGS1984Quad",
    min_z,
    max_z,
    colors: Optional[Union[Sequence[str], str]] = None,
    nodata_color: Optional[str] = None,
    reverse_palette: bool = False,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    nodata_val_fallback: float = np.nan,
) -> List[str]:
    """
    Create web tiles from a GeoTIFF.

    If min_value / max_value are None, per-band, per-tile min/max are computed
    directly from the data. If they are provided, those global values are used
    for all tiles.
    """
    tif_path_p = Path(tif_path)
    if not tif_path_p.is_file():
        raise FileNotFoundError(f"GeoTIFF not found: {tif_path_p.resolve()}")

    out_dir_p = Path(out_dir)
    out_dir_p.mkdir(parents=True, exist_ok=True)

    tms = morecantile.tms.get(tms_id)
    palette = _ensure_palette(colors, nodata_color, reverse_palette)
    written: List[str] = []

    with COGReader(str(tif_path_p), tms=tms) as reader:
        if reader.crs is None:
            raise ValueError(
                f"{tif_path_p} has no CRS/georeferencing. "
                "Assign a CRS before tiling."
            )
        bounds = getattr(reader, "geographic_bounds", reader.bounds)

        band_names = _band_names_from_reader(reader)
        band_count = reader.dataset.count
        ds_nodata = reader.dataset.nodata
        nodata_val = nodata_val_fallback

        for z in range(max_z, min_z - 1, -1):
            for tile in tiles_covering_bounds(tms, bounds, z):
                x, y = tile.x, tile.y
                try:
                    image = reader.tile(x, y, z)
                except (PointOutsideBounds, TileOutsideBounds):
                    continue

                arr = image.array.astype("float32")  # (bands, H, W)
                mask = image.mask                    # (H, W) or (bands, H, W) or None

                if mask is not None:
                    if mask.ndim == 2:
                        mask = np.broadcast_to(mask, arr.shape)
                    invalid = mask == 0  # 0 = nodata, 255 = valid
                    arr = np.where(invalid, np.nan, arr)

                for b in range(band_count):
                    band = arr[b]

                    if ds_nodata is not None and np.isfinite(ds_nodata):
                        band = np.where(band == ds_nodata, np.nan, band)

                    # Decide min/max for this band/tile
                    if min_value is None or max_value is None:
                        if not np.isfinite(band).any():
                            continue
                        band_min = float(np.nanmin(band))
                        band_max = float(np.nanmax(band))
                        if band_min == band_max:
                            continue
                    else:
                        band_min = float(min_value)
                        band_max = float(max_value)

                    masked = np.ma.masked_invalid(band)

                    webimg = WebImage(
                        image_data=masked.filled(nodata_val),
                        palette=palette,
                        min_val=band_min,
                        max_val=band_max,
                        nodata_val=nodata_val,
                    )

                    style =   band_names[b] if b < len(band_names) else f"band_{b+1}"
                    rel = Path(style) / tms_id / str(z) / str(x) / f"{y}.png"
                    dst = out_dir_p / rel
                    dst.parent.mkdir(parents=True, exist_ok=True)

                    webimg.save(str(dst))
                    written.append(str(dst))

    return written
