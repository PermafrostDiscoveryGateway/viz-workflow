import time
import uuid
import logging
import os

import geopandas as gpd
import pandas as pd
import pdgstaging

from . import Raster
from . import Palette
from . import WebImage

logger = logging.getLogger(__name__)


class RasterTiler():
    """
        Processes staged vector data into geotiffs and web-ready image tiles,
        according to the settings in a config file or dict. This RasterTiler
        class acts as the orchestrator of the other viz-raster classes, and
        coordinates the sending and receiving of information between them.
    """

    def __init__(
        self,
        config
    ):
        """
            Initialize the RasterTiler.

            Parameters
            ----------

            config : dict or str
                A dictionary of configuration settings or a path to a config
                JSON file.
        """

        self.config = pdgstaging.ConfigManager(config)
        self.tiles = pdgstaging.TilePathManager(
            **self.config.get_path_manager_config())

    def rasterize_all(self):
        """
            The main method for the RasterTiler class. Uses all the information
            provided in the config to create both GeoTiffs and web tiles at all
            of the specified z-levels.
        """

        paths = self.tiles.get_filenames_from_dir('staged')
        self.rasterize_vectors(paths)
        self.webtiles_from_all_geotiffs()

    def rasterize_vectors(
            self,
            paths,
            make_parents=True):
        """
            Given a list of files which are output from the viz-staging step,
            process them into geotiffs at the min z-level specified in the
            config (which must match the z-level of the staged tiles). The
            output geotiffs will be placed in the dir_geotiff specified in the
            config file. If the output geotiffs already exist, they will be
            overwritten.

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
        """

        if isinstance(paths, dict):
            self.tiles.add_base_dir(
                'vector', paths['path'], paths['ext'])
            paths = self.tiles.get_filenames_from_dir('vector')

        # Remove any paths that don't exist
        paths = self.tiles.remove_nonexistent_paths(paths)

        if paths is None or len(paths) == 0:
            raise ValueError('No vector files found. Check the path to the '
                             'vector files.')

        # Use the first path as a reference. Check that the z-value and the tms
        # match those that are configured. This first set of geotiff tiles to
        # make must be at the maximum z-level specified in the config.
        z = self.config.get_max_z()
        tms_id = self.config.get('tms_id')
        ref_tile_props = self.tiles.dict_from_path(paths[0])
        if(ref_tile_props.get('z') != z):
            raise ValueError(
                'z-level of the input vector tiles must match the' +
                ' z-level specified in the config file.')
        if(ref_tile_props.get('tms') != tms_id):
            raise ValueError(
                'tms of the input vector tiles must match the tms' +
                ' specified in the config file')

        # Track the parent tiles to make for each of the lowest level tiles
        parent_tiles = set()

        logger.info(
            f'Beginning rasterization of {len(paths)} vector files at z-level'
            f' {z}.'
        )

        # Assume that all vector files are staged, and all at the same z level.
        # Assume that tile paths have followed the convention configured for
        # the tilePathManager.
        for path in paths:
            tile = self.rasterize_vector(path)
            # Add the parent tile to the set of parent tiles.
            if tile is not None:
                parent_tiles.add(self.tiles.get_parent_tile(tile))

        logger.info(f'Finished rasterization of {len(paths)} vector files.')

        # Create the web tiles for the parent tiles.
        if make_parents:
            self.parent_geotiffs_from_children(parent_tiles)

    def rasterize_vector(self, path):
        """
            Given a path to an output file from the viz-staging step, create a
            GeoTIFF and save it to the configured dir_geotiff directory. If the
            output geotiff already exists, it will be overwritten.

            During this process, the min and max values (and other summary
            stats) of the data arrays that comprise the GeoTIFFs for each band
            will be tracked.

            Parameters
            ----------

            path : str
                Path to the staged vector file to rasterize.

            Returns
            -------

            morecantile.Tile or None
                The tile that was rasterized or None if there was an error.
        """

        try:

            # Get information about the tile from the path
            tile = self.tiles.tile_from_path(path)
            bounds = self.tiles.get_bounding_box(tile)
            out_path = self.tiles.path_from_tile(tile, 'geotiff')

            # Track and log the event
            id = self.__start_tracking('geotiffs_from_vectors')
            logger.info(f'Rasterizing {path} for tile {tile} to {out_path}.')

            # Check if deduplication should be performed first
            gdf = gpd.read_file(path)
            dedup_here = self.config.deduplicate_at('raster')
            dedup_method = self.config.get_deduplication_method()
            if dedup_here and (dedup_method is not None):
                dedup_config = self.config.get_deduplication_config(gdf)
                dedup = dedup_method(gdf, **dedup_config)
                gdf = dedup['keep']

            # Get properties to pass to the rasterizer
            raster_opts = self.config.get_raster_config()

            # Rasterize
            raster = Raster.from_vector(
                vector=gdf, bounds=bounds, **raster_opts)
            raster.write(out_path)

            # Track and log the end of the event
            message = f'Rasterization for tile {tile} complete.'
            self.__end_tracking(id, raster=raster, tile=tile, message=message)
            logger.info(
                f'Complete rasterization of tile {tile} to {out_path}.')

            return tile

        except Exception as e:
            message = f'Error rasterizing {path} for tile {tile}.'
            self.__end_tracking(id, tile=tile, error=e, message=message)
            return None

    def parent_geotiffs_from_children(self, tiles, recursive=True):
        """
            Make geotiffs for a list of parent tiles by merging and resampling
            the four child geotiffs that comprise each parent tile. All tiles
            passed to this method must be of the same z-level. The child
            geotiffs must be in the dir_geotiff specified in the config file.
            If the output geotiffs already exist, they will be overwritten.

            When recursive is True (default), this method will recursively call
            itself until all parent tiles have been made up to the lowest
            z-level specified in the config file.

            Parameters
            ----------
            tiles : set of morecantile.Tile
                A set of tiles. All tiles must be at the same z-level.
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

        logger.info(
            f'Start creating {len(tiles)} parent geotiffs at level {z}.')

        for tile in tiles:
            new_tile = self.parent_geotiff_from_children(tile)
            if new_tile is not None:
                parent_tiles.add(self.tiles.get_parent_tile(new_tile))

        logger.info(
            f'Finished creating {len(tiles)} parent geotiffs at level {z}.'
        )

        # Create the web tiles for the parent tiles.
        if recursive:
            self.parent_geotiffs_from_children(parent_tiles)

    def parent_geotiff_from_children(self, tile):
        """
            Make a geotiff for a parent tile by merging and resampling the four
            child geotiffs that comprise it. The child geotiffs must be in the
            dir_geotiff specified in the config file. If the output geotiff
            already exists, it will be overwritten.

            Parameters
            ----------
            tile : morecantile.Tile
                The tile to make a geotiff for.

            Returns
            -------
            morecantile.Tile or None
                The tile that was created or None if there was an error.
        """
        try:
            message = f'Creating tile {tile} from child geotiffs.'
            id = self.__start_tracking(
                'parent_geotiffs_from_children', message=message)

            # Get paths to children geotiffs that we will use to make the
            # composite, parent geotiff.
            children = self.tiles.get_child_paths(tile, base_dir='geotiff')
            children = self.tiles.remove_nonexistent_paths(children)
            out_path = self.tiles.path_from_tile(tile, base_dir='geotiff')
            bounds = self.tiles.get_bounding_box(tile)

            raster = Raster.from_rasters(
                rasters=children,
                resampling_methods=self.config.get_resampling_methods(),
                shape=self.config.get('tile_size'),
                bounds=bounds
            )
            raster.write(out_path)

            message = f'Finished creating tile {tile} from children'
            self.__end_tracking(id, raster=raster, tile=tile, message=message)

            return tile

        except Exception as e:
            logger.error(e)
            message = f'Error creating parent geotiff for tile {tile}.'
            self.__end_tracking(id, tile=tile, error=e, message=message)
            return None

    def webtiles_from_all_geotiffs(self, update_ranges=True):
        """
            Create web tiles from all geotiffs in the dir_geotiff specified in
            the config file. If the output web tiles already exist, they will
            be overwritten.

            Parameters
            ----------
            update_ranges : bool
                If True, the minimum and maximum values for each z-level in
                the config will be updated using the geotiff data that
                has already been processed with this Tiler.
        """
        geotiff_paths = self.tiles.get_filenames_from_dir('geotiff')
        self.webtiles_from_geotiffs(geotiff_paths, update_ranges)

    def webtiles_from_geotiffs(self, geotiff_paths=None, update_ranges=True):
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
        """

        # We need min and max for each z-level to create tiles with a
        # consistent color palette to value mapping. Update the config with the
        # min and max values from each z-level calculated during geotiff
        # processing
        if update_ranges:
            self.update_ranges()

        logger.info(f'Beginning creation of {len(geotiff_paths)} web tiles')

        for geotiff_path in geotiff_paths:
            self.webtile_from_geotiff(geotiff_path)

        logger.info(f'Finished creating {len(geotiff_paths)} web tiles.')

    def webtile_from_geotiff(self, geotiff_path):
        """
            Given the path to a GeoTIFF tile created by this Tiler, create a
            web tile for it and save it to the web_tiles directory.

            Parameters
            ----------
            geotiff_path : str
                The path to the GeoTiff tile.

            Returns
            -------
            tile : morecantile.Tile or None
                The tile that was created or None if an error occurred.
        """

        # We will make a set of tiles for each band/stat in the geotiff so we
        # need to get the bands.
        stats = self.config.get_stat_names()
        # Pre-create the palette for each stat
        palettes = self.config.get_palettes()
        palettes = [Palette(pal) for pal in palettes]

        try:

            raster = Raster.from_file(geotiff_path)
            image_data = raster.data
            tile = self.tiles.tile_from_path(geotiff_path)

            message = f'Creating web tile {tile} from geotiff {geotiff_path}.'
            id = self.__start_tracking(
                'webtiles_from_geotiffs', message=message)

            for i in range(len(stats)):
                stat = stats[i]
                palette = palettes[i]
                band_image_data = image_data[i]
                min_val = self.config.get_min(
                    stat=stat, z=tile.z, sub_general=True)
                max_val = self.config.get_max(
                    stat=stat, z=tile.z, sub_general=True)
                # Get the path for the output web tile
                output_path = self.tiles.path_from_tile(
                    tile, base_dir='web_tiles', style=stat)
                img = WebImage(
                    image_data=band_image_data,
                    palette=palette,
                    min_val=min_val,
                    max_val=max_val)
                img.save(output_path)

            message = f'Done creating web tile {tile}'
            self.__end_tracking(id, raster=raster, tile=tile, message=message)

            return tile

        except Exception as e:
            message = f'Error creating web tile for tile {tile} from '
            f'geotiff {geotiff_path}.'
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

        ranges = summary.groupby(['stat', 'z'], as_index=False)
        ranges = ranges.agg({'min': 'min', 'max': 'max'})
        ranges['vals'] = ranges.apply(
            lambda row: {
                row['z']: (row['min'], row['max'])
            }, axis=1)
        ranges = ranges.groupby('stat')['vals'].apply(
            lambda group: group.values)
        ranges = ranges.apply(
            lambda group: {
                int(k): v for d in group for k,
                v in d.items()})

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
        events_path = self.config.get('filename_rasterization_events')
        if events_path is not None:
            summary = pd.read_csv(events_path)
            return summary
        else:
            return None

    def get_rasters_summary(self):
        summary_path = self.config.get('filename_rasters_summary')
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
        if not hasattr(self, 'running_processes'):
            self.running_processes = {}
        self.running_processes[id] = (start_time, event_type)
        return id

    def __end_tracking(
            self,
            id=None,
            raster=None,
            tile=None,
            image=None,
            error=None,
            message=None):
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
        if not hasattr(self, 'running_processes'):
            self.running_processes = {}
        if id in self.running_processes:
            start_time, event_type = self.running_processes.pop(id)
            total_time = end_time - start_time
        else:
            raise Exception(f'No event with id {id} found.')

        event = {
            'id': id,
            'type': event_type,
            'start_time': start_time,
            'total_time': total_time,
            'end_time': end_time,
            'error': error,
            'path': None,
            'raster_summary': None,
            'tile': None,
            'z': None
        }

        if raster:
            event.update({
                'path': raster.path
            })

        if image:
            event.update({
                'path': image.path
            })

        if tile:
            event.update({
                'tile': str(tile),
                'z': tile.z
            })

        if raster and raster.summary:

            n = raster.count  # number of bands
            raster_info = raster.summary.copy()
            raster_info['path'] = [raster.path] * n
            if tile:
                raster_info['tile'] = [tile] * n
                raster_info['z'] = [tile.z] * n
            else:
                raster_info['tile'] = [None] * n
                raster_info['z'] = [None] * n

            summary_path = self.config.get('filename_rasters_summary')
            raster_info = pd.DataFrame(raster_info)
            self.__append_to_csv(summary_path, raster_info)

        # Save the event as a row in the summary CSV
        events_path = self.config.get('filename_rasterization_events')
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
        mode = 'a'
        if not os.path.isfile(path):
            header = True
            mode = 'w'
        data.to_csv(path, mode=mode, index=False, header=header)
