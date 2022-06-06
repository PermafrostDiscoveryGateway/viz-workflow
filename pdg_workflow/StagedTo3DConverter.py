
import logging
import os

import geopandas as gpd

import pdgstaging
from viz_3dtiles import Cesium3DTile, Cesium3DTileset


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

        # For now, manually add directory for the 3D tiles. We should add this
        # to the ConfigManager & TilePathManager class
        b3dm_ext = Cesium3DTile.FILE_EXT
        b3dm_dir = self.config.get('dir_3dtiles')
        self.tiles.add_base_dir('3dtiles', dir_path=b3dm_dir, ext=b3dm_ext)

    def all_staged_to_3dtiles(
        self,
        parent_json=False
    ):
        """
            Process all staged vector tiles into 3D tiles.

            Parameters
            ----------

            parent_json : bool
                If True, then a single parent tileset.json file will be created
                that encompasses all the 3D Tiles created. The children json
                files will subsequently be removed.
        """

        # Get the list of staged vector tiles
        paths = self.tiles.get_filenames_from_dir('staged')
        # Process each tile
        for path in paths:
            self.staged_to_3dtile(path)

        if parent_json:
            self.create_parent_json()

    def staged_to_3dtile(
        self,
        path
    ):

        try:

            # Read in the staged vector tile
            gdf = gpd.read_file(path)

            # Check if the gdf is empty
            if len(gdf) == 0:
                logger.warning(
                    f'Vector tile {path} is empty. 3D tile will not be'
                    ' created.')
                return

            # Get information about the tile from the path
            tile = self.tiles.tile_from_path(path)
            out_path = self.tiles.path_from_tile(tile, '3dtiles')

            # Get the filename of the tile WITHOUT the extension
            tile_filename = os.path.splitext(os.path.basename(out_path))[0]
            # Get the base of the path, without the filename
            tile_dir = os.path.dirname(out_path) + os.path.sep

            # Log the event
            logger.info(
                f'Creating 3D Tile from {path} for tile {tile} to {out_path}.')

            # Remove polygons with centroids that are outside the tile boundary
            prop_cent_in_tile = self.config.polygon_prop(
                'centroid_within_tile')
            gdf = gdf[gdf[prop_cent_in_tile]]

            # Check if deduplication should be performed
            dedup_here = self.config.deduplicate_at('3dtiles')
            dedup_method = self.config.get_deduplication_method()

            # Deduplicate if required
            if dedup_here and (dedup_method is not None):
                dedup_config = self.config.get_deduplication_config(gdf)
                dedup = dedup_method(gdf, **dedup_config)
                gdf = dedup['keep']

                # The tile could theoretically be empty after deduplication
                if len(gdf) == 0:
                    logger.warning(
                        f'Vector tile {path} is empty after deduplication.'
                        ' 3D Tile will not be created.')
                    return

            # Create & save the b3dm file
            tile3d = Cesium3DTile()
            tile3d.set_save_to_path(tile_dir)
            tile3d.set_b3dm_name(tile_filename)
            tile3d.from_geodataframe(gdf)

            # Create & save the tileset json
            tileset = Cesium3DTileset(tiles=[tile3d])
            tileset.set_save_to_path(tile_dir)
            tileset.set_json_filename(tile_filename)
            tileset.write_file()

        except Exception as e:
            logger.error(f'Error creating 3D Tile from {path}.')
            logger.error(e)

    def create_parent_json(self, remove_children=True):
        """
            Merge all the tileset json files into one main tileset.json file.

            Parameters
            ----------

            remove_children : bool
                If True, then the children json files will be removed after
                they are merged into the parent.
        """

        # Get the list of b3dm files
        b3dms = self.tiles.get_filenames_from_dir('3dtiles')

        # Extensions will be used to find the json file associated with each
        # b3dm file
        b3dm_ext = Cesium3DTile.FILE_EXT
        json_ext = '.' + Cesium3DTileset.FILE_EXT

        # Get the base directory where the 3D tiles are stored. The
        # tileset.json should be saved at the root of this directory
        dir3d = self.tiles.get_base_dir('3dtiles')['path']

        json_paths = []

        # Check that all the JSON files exist
        for b3dm in b3dms:
            # Get the JSON path
            json_path = b3dm.replace(b3dm_ext, json_ext)
            if not os.path.isfile(json_path):
                raise ValueError(f'JSON file {json_path} does not exist')
            # Add the path to the list
            json_paths.append(json_path)

        Cesium3DTileset().create_parent_json(
            json_paths=json_paths,
            save_to=dir3d,
            save_as='tileset',
            remove_children=remove_children
        )
