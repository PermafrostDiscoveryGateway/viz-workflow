import logging
import os

import geopandas as gpd
from shapely.geometry import box

import pdgstaging
from viz_3dtiles import TreeGenerator, BoundingVolumeRegion


logger = logging.getLogger(__name__)


class StagedTo3DConverter():
    """
        Processes staged vector data into Cesium 3D tiles according to the
        settings in a config file or dict. This class acts as the orchestrator
        of the other viz-3dtiles classes, and coordinates the sending and
        receiving of information between them.
    """

    def __init__(
        self,
        config
    ):
        """
            Initialize the StagedTo3DConverter class.

            Parameters
            ----------

            config : dict or str
                A dictionary of configuration settings or a path to a config
                JSON file. (See help(pdgstaging.ConfigManager))
        """

        self.config = pdgstaging.ConfigManager(config)
        self.tiles = pdgstaging.TilePathManager(
            **self.config.get_path_manager_config())

    def all_staged_to_3dtiles(
        self
    ):
        """
            Process all staged vector tiles into 3D tiles.
        """

        # Get the list of staged vector tiles
        paths = self.tiles.get_filenames_from_dir('staged')
        # Process each tile
        for path in paths:
            self.staged_to_3dtile(path)

    def staged_to_3dtile(self, path):
        """
            Convert a staged vector tile into a B3DM tile file and a matching
            JSON tileset file.

            Parameters
            ----------
            path : str
                The path to the staged vector tile.

            Returns
            -------
            tile, tileset : Cesium3DTile, Tileset
                The Cesium3DTiles and Cesium3DTileset objects
        """

        try:

            # Get information about the tile from the path
            tile = self.tiles.tile_from_path(path)
            out_path = self.tiles.path_from_tile(tile, '3dtiles')

            tile_bv = self.bounding_region_for_tile(tile)

            # Get the filename of the tile WITHOUT the extension
            tile_filename = os.path.splitext(os.path.basename(out_path))[0]
            # Get the base of the path, without the filename
            tile_dir = os.path.dirname(out_path) + os.path.sep

            # Log the event
            logger.info(
                f'Creating 3dtile from {path} for tile {tile} to {out_path}.')

            # Read in the staged vector tile
            gdf = gpd.read_file(path)

            # Check if the gdf is empty
            if len(gdf) == 0:
                logger.warning(
                    f'Vector tile {path} is empty. 3D tile will not be'
                    ' created.')
                return None, None

            # Remove polygons with centroids that are outside the tile boundary
            prop_cent_in_tile = self.config.polygon_prop(
                'centroid_within_tile')
            gdf = gdf[gdf[prop_cent_in_tile]]

            # Check if deduplication should be performed
            dedup_here = self.config.deduplicate_at('3dtiles')
            dedup_method = self.config.get_deduplication_method()

            # Deduplicate if required
            if dedup_here and (dedup_method is not None):
                prop_duplicated = self.config.polygon_prop('duplicated')
                if prop_duplicated in gdf.columns:
                    gdf = gdf[~gdf[prop_duplicated]]

                # The tile could theoretically be empty after deduplication
                if len(gdf) == 0:
                    logger.warning(
                        f'Vector tile {path} is empty after deduplication.'
                        ' 3D Tile will not be created.')
                    return None, None

            # Create & save the b3dm file
            ces_tile, ces_tileset = TreeGenerator.leaf_tile_from_gdf(
                gdf,
                dir=tile_dir,
                filename=tile_filename,
                z=self.config.get('z_coord'),
                geometricError=self.config.get('geometricError'),
                tilesetVersion=self.config.get('version'),
                boundingVolume=tile_bv
            )

            return ces_tile, ces_tileset

        except Exception as e:
            logger.error(f'Error creating 3D Tile from {path}.')
            logger.error(e)
            return None, None

    def parent_3dtiles_from_children(self, tiles, bv_limit=None):
        """
            Create parent Cesium 3D Tileset json files that point to of child
            JSON files in the tile tree hierarchy. This method will take a list
            of parent tiles and search the 3D tile directory for any children
            tiles to create.

            Parameters
            ----------
            tiles : list of morecantile.Tile
                The list of parent tiles to create.
        """

        tile_manager = self.tiles
        config_manager = self.config

        tileset_objs = []

        # Make the next level of parent tiles
        for parent_tile in tiles:
            # Get the path to the parent tile
            parent_path = tile_manager.path_from_tile(parent_tile, '3dtiles')
            # Get just the base dir without the filename
            parent_dir = os.path.dirname(parent_path)
            # Get the filename of the parent tile, without the extension
            parent_filename = os.path.basename(parent_path)
            parent_filename = os.path.splitext(parent_filename)[0]
            # Get the children paths for this parent tile
            child_paths = tile_manager.get_child_paths(parent_tile, '3dtiles')
            # Remove paths that do not exist
            child_paths = tile_manager.remove_nonexistent_paths(child_paths)
            # Get the parent bounding volume
            parent_bv = self.bounding_region_for_tile(
                parent_tile, limit_to=bv_limit)
            # If the bounding region is outside t
            # Get the version
            version = config_manager.get('version')
            # Get the geometric error
            geometric_error = config_manager.get('geometricError')
            # Create the parent tile
            tileset_obj = TreeGenerator.parent_tile_from_children_json(
                child_paths,
                dir=parent_dir,
                filename=parent_filename,
                geometricError=geometric_error,
                tilesetVersion=version,
                boundingVolume=parent_bv
            )
            tileset_objs.append(tileset_obj)

        return tileset_objs

    def bounding_region_for_tile(self, tile, limit_to=None):
        """
        For a morecantile.Tile object, return a BoundingVolumeRegion object
        that represents the bounding region of the tile.

        Parameters
        ----------
        tile : morecantile.Tile
            The tile object.
        limit_to : list of float
            Optional list of west, south, east, north coordinates to limit
            the bounding region to.

        Returns
        -------
        bv : BoundingVolumeRegion
            The bounding region object.
        """
        tms = self.tiles.tms
        bounds = tms.bounds(tile)
        bounds = gpd.GeoSeries(
            box(bounds.left, bounds.bottom, bounds.right, bounds.top),
            crs=tms.crs)
        if limit_to is not None:
            bounds_limitor = gpd.GeoSeries(
                box(limit_to[0], limit_to[1], limit_to[2], limit_to[3]),
                crs=tms.crs)
            bounds = bounds.intersection(bounds_limitor)
        bounds = bounds.to_crs(BoundingVolumeRegion.CESIUM_EPSG)
        bounds = bounds.total_bounds

        region_bv = {
            'west': bounds[0], 'south': bounds[1],
            'east': bounds[2], 'north': bounds[3],
        }
        return region_bv

    def make_top_level_tileset(self):
        """
        Create a top-level tileset.json file that sets all the min_z level
        tiles as its children. This is needed to display the tiles in Cesium
        when the min_z level has more than one tile.

        Returns
        -------
        tileset : Tileset
            The Cesium3DTileset object
        """

        tile_manager = self.tiles
        config_manager = self.config
        min_z = config_manager.get_min_z()

        # Make a parent tileset.json - this will combine the top level tiles if
        # there are 2, otherwise it will just refer to the top level tile.
        top_level_tiles = tile_manager.get_filenames_from_dir(
            '3dtiles', z=min_z)
        top_level_dir = tile_manager.get_base_dir('3dtiles')['path']

        return TreeGenerator.parent_tile_from_children_json(
            children=top_level_tiles,
            dir=top_level_dir
        )
