from pathlib import Path
from typing import List, Optional, Sequence, Union

import numpy as np
import rasterio
import morecantile
from morecantile import TileMatrixSet
from rasterio.transform import from_bounds
from rio_tiler.errors import PointOutsideBounds, TileOutsideBounds
from rio_tiler.io import Reader
from rio_tiler.mosaic import mosaic_reader
from rio_tiler.mosaic.methods import HighestMethod
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


def _band_names_from_reader(reader: Reader) -> List[str]:
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
):
    w, s, e, n = bounds
    return tms.tiles(w, s, e, n, [z])


def generate_tiles_from_tif(
    tif_path: Union[str, Path, List[Union[str, Path]]],
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

    if isinstance(tif_path, (str, Path)):
        tif_list = [str(tif_path)]
    else:
        tif_list = [str(p) for p in tif_path]

    tif_list = [p for p in tif_list if p.lower().endswith(".tif")]

    if len(tif_list) == 0:
        raise ValueError("No TIFF files passed to generate_tiles_from_tif")

    for p in tif_list:
        if not Path(p).is_file():
            raise FileNotFoundError(f"GeoTIFF not found: {p}")

    web_root = Path(out_dir)
    png_root = web_root / "png"
    tif_root = web_root / "tiff"

    png_root.mkdir(parents=True, exist_ok=True)
    if save_tiff:
        tif_root.mkdir(parents=True, exist_ok=True)

    tms = morecantile.tms.get(tms_id)
    palette = _ensure_palette(colors, nodata_color, reverse_palette)
    written: List[str] = []

    # Open first asset for metadata
    with Reader(tif_list[0]) as meta_reader:
        if meta_reader.crs is None:
            raise ValueError("Input TIFFs must have valid CRS")

        band_count = meta_reader.dataset.count
        band_names = _band_names_from_reader(meta_reader)
        ds_nodata = meta_reader.dataset.nodata
        nodata_val = ds_nodata if ds_nodata is not None else nodata_val_fallback

    # Compute union bounds across all assets
    all_bounds = []
    for asset in tif_list:
        with Reader(asset) as r:
            b = getattr(r, "geographic_bounds", r.bounds)
            all_bounds.append(b)

    w = min(b[0] for b in all_bounds)
    s = min(b[1] for b in all_bounds)
    e = max(b[2] for b in all_bounds)
    n = max(b[3] for b in all_bounds)
    bounds = (w, s, e, n)

    def _tif_tiler(asset: str, x: int, y: int, z: int, **kwargs):
        with Reader(asset) as src:
            return src.tile(x, y, z, **kwargs)

    for z in range(max_z, min_z - 1, -1):
        for tile in tiles_covering_bounds(tms, bounds, z):
            x = int(tile.x)
            y = int(tile.y)
            z_int = int(z)

            try:
                img, assets_used = mosaic_reader(
                    tif_list,
                    _tif_tiler,
                    x,
                    y,
                    z_int,
                    pixel_selection=HighestMethod(),
                )
            except (TileOutsideBounds, PointOutsideBounds):
                continue
            arr = img.data.astype("float32", copy=False)
            mask = img.mask.astype("uint8")
            arr = np.where(mask == 0, np.nan, arr)

            if save_tiff:
                left, bottom, right, top = tms.xy_bounds(tile)
                _, height, width = arr.shape
                transform = from_bounds(left, bottom, right, top, width, height)
                arr_tif = np.where(np.isnan(arr), nodata_val, arr)

                tif_rel = Path(tms_id) / str(z_int) / str(x) / f"{y}.tif"
                tif_path_tile = tif_root / tif_rel
                tif_path_tile.parent.mkdir(parents=True, exist_ok=True)

                profile = {
                    "driver": "GTiff",
                    "height": height,
                    "width": width,
                    "count": band_count,
                    "dtype": "float32",
                    "crs": meta_reader.dataset.crs,
                    "transform": transform,
                    "nodata": nodata_val,
                }

                with rasterio.open(tif_path_tile, "w", **profile) as dst:
                    dst.write(arr_tif)

            # Save PNG tiles by band
            for b in range(band_count):
                band = arr[b]

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
                png_rel = Path(style) / tms_id / str(z_int) / str(x) / f"{y}.png"
                png_path = png_root / png_rel
                png_path.parent.mkdir(parents=True, exist_ok=True)

                web_img.save(str(png_path))
                written.append(str(png_path))

    return written
