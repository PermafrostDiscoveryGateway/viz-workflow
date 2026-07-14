import time
import uuid
import logging
import os

import geopandas as gpd
import pandas as pd
import gc
from .ConfigManager import ConfigManager
import pdgstaging
import pyarrow as pa
import pyarrow.parquet as pq

from pdgraster import Raster
from pdgraster import Palette
from pdgraster import WebImage

logger = logging.getLogger(__name__)


class RasterTiler:
    """
    Processes staged vector data into geotiffs and web-ready image tiles,
    according to the settings in a config file or dict. This RasterTiler
    class acts as the orchestrator of the other viz-raster classes, and
    coordinates the sending and receiving of information between them.
    """

    def __init__(self, config):
        """
        Initialize the RasterTiler.

        Parameters
        ----------

        config : dict or str
            A dictionary of configuration settings or a path to a config
            JSON file.
        """
        if isinstance(config, ConfigManager):
            self.config = config
        else:
            self.config = ConfigManager(config)
        self.tiles = pdgstaging.TilePathManager(**self.config.get_path_manager_config())
        # Pre-create the palette for each stat
        palettes = self.config.get_palettes()
        self.palettes = [Palette(*pal) for pal in palettes]

    def rasterize_all(self, overwrite=True):
        """
        The main method for the RasterTiler class. Uses all the information
        provided in the config to create both GeoTiffs and web tiles at all
        of the specified z-levels.

        overwrite : bool
            Optional, defaults to True. If set to False, then if there is
            an existing tile at the output path created, then the Tiler
            will skip re-creating GeoTiffs / Webtiles for that tile.
        """

        paths = self.tiles.get_filenames_from_dir("staged")
        self.rasterize_vectors(paths, overwrite=overwrite)

        if self.config.is_web_tiles_enabled():
            self.webtiles_from_all_geotiffs(overwrite=overwrite)

        self.csv_to_parquet()

    def rasterize_vectors(self, paths, make_parents=True, overwrite=True):
        """
        Given a list of files which are output from the viz-staging step,
        process them into geotiffs at the min z-level specified in the
        config (which must match the z-level of the staged tiles). The
        output geotiffs will be placed in the dir_geotiff specified in the
        config file. By default, if the output geotiffs already exist, they
        will be overwritten. To change this behaviour, set overwrite to
        False.

        During this process, the min and max values of the data arrays that
        comprise the geotiffs for each band will be tracked. These ranges
        can later be access by calling get_z_ranges().

        Parameters
        ----------

        paths : list of str or dict
            A list of paths to vector files. If a dict is provided, all the
            vector files with extension specified by the 'ext' key will be
            found in the directory specified by the 'path' key.

        make_parents : bool
            Optional. If True (the default), then the parent geotiff tiles
            all the way up to the smallest z-level configured will be
            created.

        overwrite : bool
            Optional, defaults to True. If set to False, then if there is
            an existing GeoTiff tile at the output path created,
            rasterization will be skipped.
        """

        if isinstance(paths, dict):
            self.tiles.add_base_dir("vector", paths["path"], paths["ext"])
            paths = self.tiles.get_filenames_from_dir("vector")

        # Remove any paths that don't exist
        paths = self.tiles.remove_nonexistent_paths(paths)

        if paths is None or len(paths) == 0:
            raise ValueError(
                "No vector files found. Check the path to the " "vector files."
            )

        # Use the first path as a reference. Check that the z-value and the tms
        # match those that are configured. This first set of geotiff tiles to
        # make must be at the maximum z-level specified in the config.
        z = self.config.get_max_z()
        tms_id = self.config.get("tms_id")
        ref_tile_props = self.tiles.dict_from_path(paths[0])
        if ref_tile_props.get("z") != z:
            raise ValueError(
                "z-level of the input vector tiles must match the"
                + " z-level specified in the config file."
            )
        if ref_tile_props.get("tms") != tms_id:
            raise ValueError(
                "tms of the input vector tiles must match the tms"
                + " specified in the config file"
            )

        # Track the parent tiles to make for each of the lowest level tiles
        parent_tiles = set()

        logger.info(
            f"Beginning rasterization of {len(paths)} vector files at z-level" f" {z}."
        )

        # Assume that all vector files are staged, and all at the same z level.
        # Assume that tile paths have followed the convention configured for
        # the tilePathManager.
        for i, path in enumerate(paths):
            tile = self.rasterize_vector(path, overwrite=overwrite)
            # Add the parent tile to the set of parent tiles.
            if tile is not None:
                parent_tiles.add(self.tiles.get_parent_tile(tile))

            # Periodic garbage collection
            if (i + 1) % 50 == 0:
                gc.collect()

        logger.info(f"Finished rasterization of {len(paths)} vector files.")

        # Create the web tiles for the parent tiles.
        if make_parents:
            self.parent_geotiffs_from_children(parent_tiles)

    def rasterize_vector(self, path, overwrite=True):
        """
        Given a path to an output file from the viz-staging step, create a
        GeoTIFF and save it to the configured dir_geotiff directory. By
        default, if the output geotiff already exists, it will be
        overwritten. To change this behaviour, set overwrite to False.

        During this process, the min and max values (and other summary
        stats) of the data arrays that comprise the GeoTIFFs for each band
        will be tracked.

        Parameters
        ----------

        path : str
            Path to the staged vector file to rasterize.

        overwrite : bool
            Optional, defaults to True. If set to False, then if there is
            an existing GeoTiff tile at the output path created,
            rasterization will be skipped.

        Returns
        -------

        morecantile.Tile or None
            The tile that was rasterized or None if there was an error.
        """

        try:

            # Get information about the tile from the path
            tile = self.tiles.tile_from_path(path)
            out_path = self.tiles.path_from_tile(tile, "geotiff")

            if os.path.isfile(out_path) and not overwrite:
                logger.info(
                    f"Skip rasterizing {path} for tile {tile}." " Tile already exists."
                )
                return None

            bounds = self.tiles.get_bounding_box(tile)

            # Track and log the event
            # id = self.__start_tracking('geotiffs_from_vectors')
            # print(f'Using ID: {id}')

            gdf = gpd.read_file(path)

            # Check if deduplication should be performed first
            dedup_here = self.config.deduplicate_at("raster")
            dedup_method = self.config.get_deduplication_method()
            if dedup_here and dedup_method is not None:
                prop_duplicated = self.config.polygon_prop("duplicated")
                if prop_duplicated in gdf.columns:
                    gdf = gdf[~gdf[prop_duplicated]]

            # Get properties to pass to the rasterizer
            raster_opts = self.config.get_raster_config()

            # Rasterize
            raster = Raster.from_vector(vector=gdf, bounds=bounds, **raster_opts)
            if gdf is not None:
                del gdf
            raster.write(out_path)

            # Track and log the end of the event
            message = f"Rasterization for tile {tile} complete."
            self.__end_tracking(id, raster=raster, tile=tile, message=message)
            logger.info(f"Complete rasterization of tile {tile} to {out_path}.")

            del raster
            return tile

        except Exception as e:
            message = f"Error rasterizing {path} for tile {tile}."
            self.__end_tracking(id, tile=tile, error=e, message=message)
            return None

    def parent_geotiffs_from_children(self, tiles, recursive=True, overwrite=True):
        """
        Make geotiffs for a list of parent tiles by merging and resampling
        the four child geotiffs that comprise each parent tile. All tiles
        passed to this method must be of the same z-level. The child
        geotiffs must be in the dir_geotiff specified in the config file.
        If the output geotiffs already exist, they will be overwritten,
        unless the overwrite argument is set to False.

        When recursive is True (default), this method will recursively call
        itself until all parent tiles have been made up to the lowest
        z-level specified in the config file.

        Parameters
        ----------
        tiles : set of morecantile.Tile
            A set of tiles. All tiles must be at the same z-level.

        overwrite : bool
            Optional, defaults to True. If set to False, then if there is
            an existing tile at the output path for the parent tile, then
            the Tiler will skip creating that GeoTiff.
        """
        # tiles may be a set
        tiles = list(tiles)

        if len(tiles) == 0:
            return

        # Check which z-level we are on, using the first tile as a reference.
        z = tiles[0].z
        # Get the last z-level to make tiles for
        last_z = self.config.get_min_z()

        if z < 0 or z < last_z:
            # Tiling is complete
            return

        # Get the parent tiles from the current set of tiles.
        parent_tiles = set()

        logger.info(f"Start creating {len(tiles)} parent geotiffs at level {z}.")

        for i, tile in enumerate(tiles):
            new_tile = self.parent_geotiff_from_children(tile, overwrite=overwrite)
            if new_tile is not None:
                parent_tiles.add(self.tiles.get_parent_tile(new_tile))

            # Periodic garbage collection
            if (i + 1) % 50 == 0:
                gc.collect()

        logger.info(f"Finished creating {len(tiles)} parent geotiffs at level {z}.")

        # Create the web tiles for the parent tiles.
        if recursive:
            self.parent_geotiffs_from_children(parent_tiles, overwrite=overwrite)

    def parent_geotiff_from_children(self, tile, overwrite=True):
        """
        Make a geotiff for a parent tile by merging and resampling the four
        child geotiffs that comprise it. The child geotiffs must be in the
        dir_geotiff specified in the config file. If the output geotiff
        already exists, it will be overwritten, unless overwrite is set
        to False.

        Parameters
        ----------
        tile : morecantile.Tile
            The tile to make a geotiff for.

        overwrite : bool
            Optional, defaults to True. If set to False, then if there is
            an existing tile at the output path for the parent tile, then
            the Tiler will skip creating that GeoTiff.

        Returns
        -------
        morecantile.Tile or None
            The tile that was created or None if there was an error.
        """
        try:

            out_path = self.tiles.path_from_tile(tile, base_dir="geotiff")
            if os.path.isfile(out_path) and not overwrite:
                logger.info(
                    f"Skip making parent GeoTIFF tile {tile}." " Tile already exists."
                )
                return None

            message = f"Creating tile {tile} from child geotiffs."
            # id = self.__start_tracking(
            #    'parent_geotiffs_from_children', message=message)

            # Get paths to children geotiffs that we will use to make the
            # composite, parent geotiff.
            children = self.tiles.get_child_paths(tile, base_dir="geotiff")
            children = self.tiles.remove_nonexistent_paths(children)
            bounds = self.tiles.get_bounding_box(tile)

            raster = Raster.from_rasters(
                rasters=children,
                resampling_methods=self.config.get_resampling_methods(),
                shape=self.config.get("tile_size"),
                bounds=bounds,
            )
            raster.write(out_path)

            message = f"Finished creating tile {tile} from children"
            self.__end_tracking(id, raster=raster, tile=tile, message=message)

            return tile

        except Exception as e:
            logger.error(e)
            message = f"Error creating parent geotiff for tile {tile}."
            self.__end_tracking(id, tile=tile, error=e, message=message)
            return None

    def webtiles_from_all_geotiffs(self, update_ranges=True, overwrite=True):
        """
        Create web tiles from all geotiffs in the dir_geotiff specified in
        the config file. If the output web tiles already exist, they will
        be overwritten, unless overwrite is set to False.

        Parameters
        ----------
        update_ranges : bool
            If True, the minimum and maximum values for each z-level in the
            config will be updated using the geotiff data that has already
            been processed with this Tiler.

        overwrite : bool
            Optional, defaults to True. If set to False, then if there is
            an existing image tile at the output path for the web tile,
            then the Tiler will skip it.
        """
        geotiff_paths = self.tiles.get_filenames_from_dir("geotiff")
        self.webtiles_from_geotiffs(geotiff_paths, update_ranges, overwrite)

    def webtiles_from_geotiffs(
        self, geotiff_paths=None, update_ranges=True, overwrite=True
    ):
        """
        Create web tiles given a list of geotiffs

        Parameters
        ----------
        geotiff_paths : list of str
            A list of paths to geotiffs. Paths must follow the convention
            configured for the tilePathManager.
        update_ranges : bool
            If True, the minimum and maximum values for each z-level in
            the config will be updated using the geotiff data that
            has already been processed with this Tiler.
        overwrite : bool
            Optional, defaults to True. If set to False, then if there is
            an existing image tile at the output path for the that tile,
            then the Tiler will skip it.
        """

        # We need min and max for each z-level to create tiles with a
        # consistent color palette to value mapping. Update the config with the
        # min and max values from each z-level calculated during geotiff
        # processing
        if update_ranges:
            self.update_ranges()

        logger.info(f"Beginning creation of {len(geotiff_paths)} web tiles")

        for i, geotiff_path in enumerate(geotiff_paths):
            self.webtile_from_geotiff(geotiff_path, overwrite=overwrite)
            # Periodic garbage collection
            if (i + 1) % 50 == 0:
                gc.collect()

        logger.info(f"Finished creating {len(geotiff_paths)} web tiles.")

    def webtile_from_geotiff(self, geotiff_path, overwrite=True):
        """
        Given the path to a GeoTIFF tile created by this Tiler, create a
        web tile for it and save it to the web_tiles directory.

        Parameters
        ----------
        geotiff_path : str
            The path to the GeoTiff tile.
        overwrite : bool
            Optional, defaults to True. If set to False, then if there is
            an existing image tile at the output path for the tile,
            then the Tiler will skip creating that image tile.

        Returns
        -------
        tile : morecantile.Tile or None
            The tile that was created or None if an error occurred.
        """

        # We will make a set of tiles for each band/stat in the geotiff so we
        # need to get the bands.
        stats = self.config.get_stat_names()
        palettes = self.palettes
        nodata_vals = self.config.get_nodata_vals()

        try:
            raster = Raster.from_file(geotiff_path)
            image_data = raster.data
            tile = self.tiles.tile_from_path(geotiff_path)

            message = f"Creating web tile {tile} from geotiff {geotiff_path}."
            # id = self.__start_tracking(
            #    'webtiles_from_geotiffs', message=message)

            for i in range(len(stats)):
                stat = stats[i]
                # Get the path for the output web tile
                output_path = self.tiles.path_from_tile(
                    tile, base_dir="web_tiles", style=stat
                )
                if os.path.isfile(output_path) and not overwrite:
                    logger.info(
                        f"Skip creating web tile for tile {tile} and "
                        f"stat {stat}. Web tile already exists at "
                        f"{output_path}"
                    )
                    continue
                palette = palettes[i]
                nodata_val = nodata_vals[i]
                band_image_data = image_data[i]
                min_val = self.config.get_min(stat=stat, z=tile.z, sub_general=True)
                max_val = self.config.get_max(stat=stat, z=tile.z, sub_general=True)
                img = WebImage(
                    image_data=band_image_data,
                    palette=palette,
                    min_val=min_val,
                    max_val=max_val,
                    nodata_val=nodata_val,
                )
                img.save(output_path)

            message = f"Done creating web tile {tile}"
            self.__end_tracking(id, raster=raster, tile=tile, message=message)

            del raster
            del image_data
            return tile

        except Exception as e:
            message = f"Error creating web tile for tile {tile} from "
            f"geotiff {geotiff_path}."
            self.__end_tracking(id, tile=tile, error=e, message=message)
            return None

    def get_z_ranges(self):
        """
        For the z-levels and geotiffs that have been processed by this
        tiler so far, get the min and max values for each z-level.

        Returns
        -------
        dict
            A dictionary of z-level: (min, max) tuples for each statistic.
        """

        summary = self.get_rasters_summary()

        # Convert to dict in format:
        # { 'stat' : { 'z': (min, max), ... } ... }

        ranges = summary.groupby(["stat", "z"], as_index=False)
        ranges = ranges.agg({"min": "min", "max": "max"})
        ranges["vals"] = ranges.apply(
            lambda row: {row["z"]: (row["min"], row["max"])}, axis=1
        )
        ranges = ranges.groupby("stat")["vals"].apply(lambda group: group.values)
        ranges = ranges.apply(
            lambda group: {int(k): v for d in group for k, v in d.items()}
        )

        return ranges.to_dict()

    def update_ranges(self):
        """
        Get the min and max values that exist in the GeoTIFF data that has
        been created by this tiler so far. Update the config with the new
        ranges, if any are missing from the config. The min & max are found
        by searching the raster summary table for the minimum and maximum
        values within z levels.
        """
        new_ranges = self.get_z_ranges()
        self.config.update_ranges(new_ranges)

    def get_errors(self):
        """
        Get a list of errors that occurred during processing.

        Returns
        -------
        pandas.DataFrame
            A dataframe containing the errors that occurred during
            processing.
        """
        df = self.get_events()
        return df[df.error.notnull()]

    def get_events(self):
        """
        Return a summary of events, including raster summary stats, as a
        dataframe.

        Returns
        -------
        pandas.DataFrame
            A pandas dataframe summarizing events.
        """
        events_path = self.config.get("filename_rasterization_events")
        if events_path is not None:
            summary = pd.read_csv(events_path)
            return summary
        else:
            return None

    def get_rasters_summary(self):
        summary_path = self.config.get("filename_rasters_summary")
        if summary_path is not None:
            summary = pd.read_csv(summary_path)
            return summary
        else:
            return None

    def __start_tracking(self, event_type=None, message=None):
        """
        Start timing an event in the rasterization process.

        Parameters
        ----------
        event_type : str
            The name of the method that called this method.
        message : str
            An optional message to log.
        """

        if message:
            logger.info(message)

        start_time = time.time()
        id = str(uuid.uuid4())
        if not hasattr(self, "running_processes"):
            self.running_processes = {}
        self.running_processes[id] = (start_time, event_type)
        return id

    def __end_tracking(
        self,
        id="id_replacement",
        raster=None,
        tile=None,
        image=None,
        error=None,
        message=None,
    ):
        """
        Stop timing an event in the rasterization process. Record the
        elapsed time and other info about the event, and save summary data
        from the raster that was created (if applicable).

        Parameters
        ----------
        id : str
            The id, created with __start_tracking, of the event to end.
        raster : Raster
            The raster that was processed, if applicable.
        tile : morecantile.Tile
            The tile that was processed, if applicable.
        image : WebImage
            The image that was processed, if applicable.
        error : Exception
            The error that occurred, if applicable.
        """

        if message:
            if error:
                logger.error(message)
                logger.error(error)
            else:
                logger.info(message)

        total_time = None
        end_time = time.time()
        if not hasattr(self, "running_processes"):
            self.running_processes = {}
        # if id in self.running_processes:
        #     start_time, event_type = self.running_processes.pop(id)
        #     total_time = end_time - start_time
        # else:
        #     raise Exception(f'No event with id {id} found.')

        # replacement code for chunk above
        # do not pull in variable id bc cannot get it working with ray
        event_type = "event_type_replacement"
        start_time = "start_time_replacement"

        event = {
            "id": id,
            "type": event_type,
            "start_time": start_time,
            "total_time": total_time,
            "end_time": end_time,
            "error": error,
            "path": None,
            "raster_summary": None,
            "tile": None,
            "z": None,
        }

        if raster:
            event.update({"path": raster.path})

        if image:
            event.update({"path": image.path})

        if tile:
            event.update({"tile": str(tile), "z": tile.z})

        if raster and raster.summary:

            n = raster.count  # number of bands
            raster_info = raster.summary.copy()
            raster_info["path"] = [raster.path] * n
            if tile:
                raster_info["tile"] = [tile] * n
                raster_info["z"] = [tile.z] * n
            else:
                raster_info["tile"] = [None] * n
                raster_info["z"] = [None] * n

            summary_path = self.config.get("filename_rasters_summary")
            raster_info = pd.DataFrame(raster_info)
            self.__append_to_csv(summary_path, raster_info)

        # Save the event as a row in the summary CSV
        events_path = self.config.get("filename_rasterization_events")
        event = pd.DataFrame(event, index=[0])
        self.__append_to_csv(events_path, event)

    def __append_to_csv(self, path, data):
        """
        Add data from a dataframe to a CSV file. If the CSV doesn't exist
        yet, create it along with the column header row.

        Parameters
        ----------
        path : str
            The path to the CSV file.
        data : pandas.DataFrame
            The data to add to the CSV.
        """
        header = False
        mode = "a"
        if not os.path.isfile(path):
            header = True
            mode = "w"
        data.to_csv(path, mode=mode, index=False, header=header)

    def csv_to_parquet(self):
        """
        This method is to converst the CSV files containing rasterization events and raster
        summaries to Parquet format, keeping the original CSVs.
        """
        csv_paths = [
            self.config.get("filename_rasterization_events"),
            self.config.get("filename_rasters_summary"),
        ]

        for csv_path in filter(None, csv_paths):
            if not os.path.isfile(csv_path):
                self.logger.warning(f"CSV not found → {csv_path}")
                continue

            root, _ = os.path.splitext(csv_path)
            parquet_path = f"{root}.parquet"

            df = pd.read_csv(csv_path)
            pq.write_table(
                pa.Table.from_pandas(df, preserve_index=False),
                parquet_path,
                compression="snappy",
            )
            del df
            gc.collect()

    def rasterize_max_z_level(self, file_paths=None, overwrite=True):
        """
        Wrapper to rasterize only the maximum z-level tiles from staged vectors.

        This method processes all staged vector tiles at the maximum configured
        z-level and converts them to GeoTIFF rasters. Parent tiles are NOT created
        in this step - they will be created in subsequent steps using
        rasterize_composite_z_level().

        Parameters
        ----------
        file_paths : list of str, optional
            List of file paths to staged vector tiles. If not provided, all files
            from the configured staged directory will be discovered automatically.
        overwrite : bool, optional
            Defaults to True. If set to False, then if there is an existing
            GeoTiff tile at the output path, rasterization will be skipped.

        Returns
        -------
        int
            The number of tiles successfully rasterized.

        Raises
        ------
        ValueError
            If no staged vector files are found or if z-level mismatch occurs.

        Example
        -------
        >>> tiler = RasterTiler(config)
        >>> # Auto-discovery mode
        >>> tiles_processed = tiler.rasterize_max_z_level(overwrite=True)
        >>> print(f"Processed {tiles_processed} tiles at max z-level")
        >>>
        >>> # Explicit file paths mode
        >>> paths = ['path/to/tile1.geojson', 'path/to/tile2.geojson']
        >>> tiles_processed = tiler.rasterize_max_z_level(file_paths=paths)
        """
        logger.info("Starting rasterization for maximum z-level")

        # Get file paths from parameter or discover from directory
        if file_paths is not None:
            logger.info(f"Using {len(file_paths)} provided file paths")
            paths = file_paths
        else:
            logger.info(
                "Auto-discovering staged vector files from configured directory"
            )
            paths = self.tiles.get_filenames_from_dir("staged")

        # Validate paths exist
        paths = self.tiles.remove_nonexistent_paths(paths)
        if not paths:
            raise ValueError(
                "No vector files found. Check the path to the staged vector files."
            )

        # Validate z-level and TMS
        z = self.config.get_max_z()
        tms_id = self.config.get("tms_id")
        ref_tile_props = self.tiles.dict_from_path(paths[0])

        if ref_tile_props.get("z") != z:
            raise ValueError(
                f"z-level of the input vector tiles ({ref_tile_props.get('z')}) "
                f"must match the max z-level specified in the config ({z})."
            )
        if ref_tile_props.get("tms") != tms_id:
            raise ValueError(
                f"tms of the input vector tiles ({ref_tile_props.get('tms')}) "
                f"must match the tms specified in the config ({tms_id})."
            )

        logger.info(
            f"Beginning rasterization of {len(paths)} vector files at z-level {z}."
        )

        # Rasterize all tiles at max z-level (without creating parents)
        tiles_processed = 0
        for path in paths:
            tile = self.rasterize_vector(path, overwrite=overwrite)
            if tile is not None:
                tiles_processed += 1

        logger.info(
            f"Completed rasterization of {tiles_processed} tiles at z-level {z}."
        )

        return tiles_processed

    def rasterize_composite_z_level(self, z, file_paths=None, overwrite=True):
        """
        Wrapper to create composite (parent) GeoTIFF tiles for a specific z-level.

        This method creates parent tiles at the specified z-level by compositing
        the four child tiles from z+1. This should be called sequentially for each
        z-level from (max_z - 1) down to min_z to ensure all parent tiles exist
        before creating their own parents.

        Run this for each z-level in sequence, ensuring z+1 completes before
        starting z. Can parallelize within a z-level across tiles.

        Parameters
        ----------
        z : int
            The z-level to create composite tiles for. Must be less than max_z.
        file_paths : list of str, optional
            List of file paths to child GeoTIFF tiles at z+1. If not provided,
            all child tiles from the configured geotiff directory will be
            discovered automatically.
        overwrite : bool, optional
            Defaults to True. If set to False, existing GeoTiff tiles will
            not be recreated.

        Returns
        -------
        int
            The number of parent tiles successfully created at this z-level.

        Raises
        ------
        ValueError
            If z is invalid (>= max_z or < min_z).

        Example
        -------
        >>> tiler = RasterTiler(config)
        >>> # First process max z-level
        >>> tiler.rasterize_max_z_level()
        >>> # Then process each composite level sequentially (auto-discovery)
        >>> for z in range(max_z - 1, min_z - 1, -1):
        >>>     tiles_created = tiler.rasterize_composite_z_level(z)
        >>>     print(f"Created {tiles_created} tiles at z-level {z}")
        >>>
        >>> # Or with explicit file paths
        >>> child_paths = ['path/to/child1.tif', 'path/to/child2.tif']
        >>> tiles_created = tiler.rasterize_composite_z_level(z=10, file_paths=child_paths)
        """
        max_z = self.config.get_max_z()
        min_z = self.config.get_min_z()

        # Validate z-level
        if z >= max_z:
            raise ValueError(
                f"z-level {z} must be less than max_z ({max_z}). "
                "Use rasterize_max_z_level() for the maximum z-level."
            )
        if z < min_z:
            raise ValueError(
                f"z-level {z} is below min_z ({min_z}). No tiles to create."
            )

        logger.info(f"Starting composite tile creation for z-level {z}")

        # Get child tile paths from parameter or discover from directory
        child_z = z + 1
        if file_paths is not None:
            logger.info(f"Using {len(file_paths)} provided child tile paths")
            child_paths = file_paths
        else:
            logger.info(f"Auto-discovering child GeoTIFF tiles at z-level {child_z}")
            child_paths = self.tiles.get_filenames_from_dir("geotiff", z=child_z)

        child_paths = self.tiles.remove_nonexistent_paths(child_paths)

        if not child_paths:
            logger.warning(
                f"No child GeoTIFF tiles found at z-level {child_z}. "
                "Ensure previous z-level processing completed successfully."
            )
            return 0

        # Get unique parent tiles from all children
        parent_tiles = set()
        for child_path in child_paths:
            child_tile = self.tiles.tile_from_path(child_path)
            parent_tile = self.tiles.get_parent_tile(child_tile)
            # Only add if parent is at the target z-level
            if parent_tile.z == z:
                parent_tiles.add(parent_tile)

        logger.info(
            f"Creating {len(parent_tiles)} parent tiles at z-level {z} "
            f"from {len(child_paths)} child tiles at z-level {child_z}."
        )

        # Create each parent tile
        tiles_created = 0
        for tile in parent_tiles:
            created_tile = self.parent_geotiff_from_children(tile, overwrite=overwrite)
            if created_tile is not None:
                tiles_created += 1

        logger.info(
            f"Completed creation of {tiles_created} parent tiles at z-level {z}."
        )

        return tiles_created

    def create_web_tiles_for_z_level(
        self, z, file_paths=None, update_ranges=True, overwrite=True
    ):
        """
        Wrapper to create web tiles for a specific z-level from GeoTIFF tiles.

        This method creates web image tiles (PNG) from GeoTIFF tiles at the
        specified z-level. Can be run independently for each z-level after all
        GeoTIFF tiles have been created (both max z and all composite levels).

        Run this for each z-level in parallel once all GeoTIFF processing is
        complete. Can also parallelize within a z-level across tiles.

        Parameters
        ----------
        z : int
            The z-level to create web tiles for.
        file_paths : list of str, optional
            List of file paths to GeoTIFF tiles. If not provided, all GeoTIFF
            tiles at the specified z-level from the configured directory will
            be discovered automatically.
        update_ranges : bool, optional
            If True, the minimum and maximum values for this z-level will be
            updated from the raster summary data before creating web tiles.
            This ensures consistent color mapping. Defaults to True.
        overwrite : bool, optional
            Defaults to True. If set to False, existing web tiles will not
            be recreated.

        Returns
        -------
        int
            The number of web tiles successfully created at this z-level.

        Raises
        ------
        ValueError
            If z is invalid or no GeoTIFF tiles exist at this z-level.

        Example
        -------
        >>> tiler = RasterTiler(config)
        >>> # After all GeoTIFF processing is complete (auto-discovery)
        >>> for z in range(min_z, max_z + 1):
        >>>     tiles_created = tiler.create_web_tiles_for_z_level(z)
        >>>     print(f"Created {tiles_created} web tiles at z-level {z}")
        >>>
        >>> # Or with explicit file paths
        >>> geotiff_paths = ['path/to/tile1.tif', 'path/to/tile2.tif']
        >>> tiles_created = tiler.create_web_tiles_for_z_level(
        >>>     z=10, file_paths=geotiff_paths
        >>> )
        """
        max_z = self.config.get_max_z()
        min_z = self.config.get_min_z()

        # Validate z-level
        if z > max_z or z < min_z:
            raise ValueError(f"z-level {z} is outside valid range [{min_z}, {max_z}].")

        logger.info(f"Starting web tile creation for z-level {z}")

        # Update ranges if requested
        if update_ranges:
            logger.info("Updating value ranges from raster summary data")
            self.update_ranges()

        # Get GeoTIFF paths from parameter or discover from directory
        if file_paths is not None:
            logger.info(f"Using {len(file_paths)} provided GeoTIFF paths")
            geotiff_paths = file_paths
        else:
            logger.info(f"Auto-discovering GeoTIFF tiles at z-level {z}")
            geotiff_paths = self.tiles.get_filenames_from_dir("geotiff", z=z)

        geotiff_paths = self.tiles.remove_nonexistent_paths(geotiff_paths)

        if not geotiff_paths:
            raise ValueError(
                f"No GeoTIFF tiles found at z-level {z}. "
                "Ensure GeoTIFF processing completed successfully."
            )

        logger.info(
            f"Creating web tiles for {len(geotiff_paths)} GeoTIFF tiles at z-level {z}."
        )

        # Create web tiles
        tiles_created = 0
        for geotiff_path in geotiff_paths:
            tile = self.webtile_from_geotiff(geotiff_path, overwrite=overwrite)
            if tile is not None:
                tiles_created += 1

        logger.info(f"Completed creation of {tiles_created} web tiles at z-level {z}.")

        return tiles_created
