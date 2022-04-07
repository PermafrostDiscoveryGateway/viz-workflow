import time
import uuid
import json
import logging

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
        cent_x = self.config.polygon_prop('centroid_x')
        cent_y = self.config.polygon_prop('centroid_y')
        centroid_properties = (cent_x, cent_y)
        self.rasterize_vectors(paths, centroid_properties)
        self.webtiles_from_all_geotiffs()

    def rasterize_vectors(
            self,
            paths,
            centroid_properties=None,
            make_parents=True):
        """
            Given a list of files which are output from the viz-staging step,
            process them into geotiffs at the min z-level specified in the
            config (which must match the z-level of the staged tiles). The
            output geotiffs will be placed in the geotiff_dir specified in the
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

            centroid_properties : tuple of str
                Optional. If centroids have been pre-calculated for these
                vectors, then the name the two properties in the vector data
                that contain the x-coordinates and y-coordinates, respectively,
                of the centroids for each Polygon. If set to None, then the
                centroids are calculated from the geometry. Centroid
                coordinates are assumed be in the same CRS as the Polygon
                geometry of the vector file.

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

        # Get properties to pass to the rasterizer
        raster_stats_config = self.config.get_raster_config()
        tile_size = self.config.get('tile_size')

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

            try:

                id = self.__start_tracking('geotiffs_from_vectors')

                # Get information about the tile from the path
                tile = self.tiles.tile_from_path(path)
                bounds = self.tiles.get_bounding_box(tile)
                out_path = self.tiles.path_from_tile(tile, 'geotiff')

                logger.info(
                    f'Rasterizing {path} for tile {tile} to {out_path}.')

                # Check if deduplication should be performed first
                gdf = gpd.read_file(path)
                if self.config.deduplicate_at('raster'):
                    dedup = pdgstaging.deduplicate(
                        gdf, **self.config.get_deduplication_config())
                    gdf = dedup['keep']

                raster = Raster.from_vector(
                    vector=gdf,
                    centroid_properties=centroid_properties,
                    bounds=bounds,
                    shape=tile_size,
                    stats=raster_stats_config
                )
                raster.write(out_path)

                # Add the parent tile to the set of parent tiles.
                parent_tiles.add(
                    self.tiles.get_parent_tile(tile)
                )
                self.__end_tracking(
                    id,
                    raster=raster,
                    tile=tile,
                    message=f'Rasterization for tile {tile} complete.')

            except Exception as e:
                self.__end_tracking(
                    id,
                    tile=tile,
                    error=e,
                    message=f'Error rasterizing {path} for tile {tile}.')
                continue

        logger.info(
            f'Finished rasterization of {len(paths)} vector files into'
            f' geotiffs.'
        )

        # Create the web tiles for the parent tiles.
        if make_parents:
            self.parent_geotiffs_from_children(parent_tiles)

    def parent_geotiffs_from_children(self, tiles, recursive=True):
        """
            Make geotiffs for a list of parent tiles by merging and resampling
            the four child geotiffs that comprise each parent tile. All tiles
            passed to this method must be of the same z-level. The child
            geotiffs must be in the geotiff_dir specified in the config file.
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

        # Get the shape of the tile
        shape = self.config.get('tile_size')
        resampling_methods = self.config.get_resampling_methods()

        # Get the parent tiles from the current set of tiles.
        parent_tiles = set()

        logger.info(
            f'Start creating {len(tiles)} parent geotiffs at level {z}.')

        for tile in tiles:
            try:
                id = self.__start_tracking(
                    'parent_geotiffs_from_children',
                    message=f'Creating tile {tile} from child geotiffs.')

                children = self.tiles.get_child_paths(tile, base_dir='geotiff')
                children = self.tiles.remove_nonexistent_paths(children)
                out_path = self.tiles.path_from_tile(tile, base_dir='geotiff')
                bounds = self.tiles.get_bounding_box(tile)

                raster = Raster.from_rasters(
                    rasters=children,
                    resampling_methods=resampling_methods,
                    shape=shape,
                    bounds=bounds
                )
                raster.write(out_path)

                parent_tiles.add(
                    self.tiles.get_parent_tile(tile)
                )
                self.__end_tracking(
                    id,
                    raster=raster,
                    tile=tile,
                    message=f'Finished creating tile {tile} from children')
            except Exception as e:
                logger.error(e)
                self.__end_tracking(
                    id,
                    tile=tile,
                    error=e,
                    message=f'Error creating parent geotiff for tile {tile}.')
                continue

        logger.info(
            f'Finished creating {len(tiles)} parent geotiffs at level {z}.'
        )

        # Create the web tiles for the parent tiles.
        if recursive:
            self.parent_geotiffs_from_children(parent_tiles)

    def webtiles_from_all_geotiffs(self, update_ranges=True):
        """
            Create web tiles from all geotiffs in the geotiff_dir specified in
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
            new_ranges = self.get_z_ranges()
            self.config.update_ranges(new_ranges)

        # We will make a set of tiles for each band/stat in the geotiff so we
        # need to get the bands.
        stats = self.config.get_stat_names()
        # Pre-create the palette for each stat
        palettes = self.config.get_palettes()
        palettes = [Palette(pal) for pal in palettes]

        logger.info(
            f'Beginning creation of {len(geotiff_paths)} web tiles '
            f'from {len(stats)} bands.'
        )

        for gt_path in geotiff_paths:
            try:

                raster = Raster.from_file(gt_path)
                image_data = raster.data
                tile = self.tiles.tile_from_path(gt_path)

                id = self.__start_tracking(
                    'webtiles_from_geotiffs',
                    message=f'Creating web tile {tile} from {gt_path}.')

                # single band example
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
                self.__end_tracking(
                    id,
                    raster=raster,
                    tile=tile,
                    message=f'Done creating web tiles from geotiff {gt_path}.')
            except Exception as e:
                self.__end_tracking(
                    id,
                    tile=tile,
                    error=e,
                    message=f'Error creating web tile for tile {tile} from '
                            f'geotiff {gt_path}.')
                continue

    def get_z_ranges(self):
        """
            For the z-levels and geotiffs that have been processed by this
            tiler so far, get the min and max values for each z-level.

            Returns
            -------
            dict
                A dictionary of z-level: (min, max) tuples for each statistic.
        """

        ranges = {}

        for event in self.events:
            raster_summary = event.get('raster_summary')
            if raster_summary is not None:
                z = event.get('z')
                for stat, summary in raster_summary.items():
                    if ranges.get(stat) is None:
                        ranges[stat] = {}
                    if ranges[stat].get(z) is None:
                        ranges[stat][z] = []
                    ranges[stat][z].extend(
                        (summary.get('min'), summary.get('max')))

        for stat, zs in ranges.items():
            for z, values in zs.items():
                ranges[stat][z] = (
                    min(i for i in values if i is not None),
                    max(i for i in values if i is not None)
                )

        return ranges

    def get_errors(self):
        """
            Get a list of errors that occurred during processing.

            Returns
            -------
            list
                A list of errors.
        """
        return [e for e in self.events if e['error'] is not None]

    def get_events(self, as_df=False):
        """
            Return the events

            Parameters
            ----------
            as_df : bool
                If True, return the events as a pandas dataframe. If False,
                return the events as a list of dictionaries.

            Returns
            -------
            list or pandas.DataFrame
                A list of events or a pandas dataframe summarizing events.
        """
        if as_df:
            return pd.DataFrame(self.events)
        return self.events

    def save_events(self, path, as_csv=False):
        """
            Save the events to a file.

            Parameters
            ----------
            path : str
                The path to the file to save the events to.
            as_csv : bool
                If True, save the events as a CSV. If False, save the events
                as a JSON.
        """
        self.tiles.create_dirs(path)
        if as_csv:
            self.get_events(as_df=True).to_csv(path)
        else:
            with open(path, 'w') as f:
                json.dump(self.get_events(), f)

    def __start_tracking(self, event_type=None, message=None):
        """
            Start a new event.

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
            End an event.

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

        if not hasattr(self, 'events'):
            self.events = []

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
                'path': raster.path,
                'raster_summary': raster.summary,
            })

        if image:
            event.update({
                'path': image.path
            })

        if tile:
            event.update({
                'tile': tile,
                'z': tile.z
            })

        self.events.append(event)
