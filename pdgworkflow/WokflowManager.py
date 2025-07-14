"""
Workflow Manager for PDG Visualization workflow.

This module provides the WokflowManager class for orchestrating
the complete visualization workflow including staging, rasterization,
and tile generation.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# Third-party imports
import click

# Local imports
from .ConfigManager import ConfigManager
from .RasterTiler import RasterTiler
from .StagedTo3DConverter import StagedTo3DConverter
from .WMTSCapabilitiesGenerator import WMTSCapabilitiesGenerator
from pdgstaging import TileStager
from pdgstaging import TilePathManager


# Set up logging
logger = logging.getLogger(__name__)


class WokflowManagerError(Exception):
    """Custom exception for WokflowManager errors."""

    pass


class WokflowManager:
    """
    Workflow Manager for orchestrating the complete PDG visualization workflow.

    This class manages the entire visualization workflow including:
    - Data staging and preprocessing
    - Rasterization of vector data
    - Tile generation for web mapping
    - 3D tile conversion
    - WMTS capabilities generation

    Args:
        config: Configuration dictionary or ConfigManager instance

    Attributes:
        config (ConfigManager): Configuration manager instance

    Raises:
        WokflowManagerError: When workflow operations fail
        ValueError: When invalid parameters are provided
        TypeError: When incorrect types are provided

    Example:
        >>> config = {"dir_input": "/path/to/input"}
        >>> workflow = WokflowManager(config)
        >>> workflow.run_workflow()
    """

    def __init__(
        self,
        config: Union[Dict[str, Any], ConfigManager, None] = None,
        *,
        log_level: str = "INFO",
    ) -> None:
        """
        Initialize the WokflowManager.

        Args:
            config: Configuration dictionary or ConfigManager instance
            validate_config: Whether to validate configuration on initialization
            log_level: Logging level for the workflow manager

        Raises:
            ValueError: If config is invalid
            TypeError: If config is wrong type
        """
        self.config = ConfigManager(config)

        self.tiles = TilePathManager(**self.config.get_path_manager_config())

        # Configured names of properties that will be added to each polygon
        # during either staging or rasterization
        self.props = self.config.props

        # Create tiles for the maximum z-level configured
        self.z_level = self.config.get_max_z()

    def run_staging(self) -> bool:
        """
        Run the data staging step of the workflow.

        Args:
            None

        Returns:
            True if staging completed successfully

        Raises:
            WokflowManagerError: If staging fails
            FileNotFoundError: If input directory doesn't exist
        """
        self.tile_stager = self.init_tiler()

        return self.tile_stager.stage_all()

    def init_tiler(self) -> TileStager:
        """
        Initialize the TileStager instance with configured tiles and properties.

        Args:
            None

        Returns:
            TileStager instance configured with tiles and properties

        Raises:
            WokflowManagerError: If initialization fails
            FileNotFoundError: If input directory doesn't exist
        """
        return TileStager(
            tiles=self.tiles,
            props=self.props,
            z_level=self.z_level,
        )

    def stage_all(self) -> bool:
        """
        Stage all tiles using the provided TileStager instance.

        Args:
            None

        Returns:
            True if staging completed successfully

        Raises:
            WokflowManagerError: If staging fails
            FileNotFoundError: If input directory doesn't exist
        """
        return self.tile_stager.stage_all()

    def stage(self, path: str) -> bool:
        """
        Stage a single tile using the provided TileStager instance.

        Args:
            path: Tile path to stage

        Returns:
            True if staging completed successfully for the specified tile

        Raises:
            WokflowManagerError: If staging fails
            FileNotFoundError: If input directory doesn't exist
        """
        return self.tile_stager.stage(path=path)

    def run_rasterization(self) -> bool:
        """
        Run the rasterization step of the workflow.

        Args:
            None

        Returns:
            True if rasterization completed successfully

        Raises:
            WokflowManagerError: If rasterization fails
        """
        self.raster_tiler = self.init_raster_tiler()

        return self.raster_tiler.rasterize_all(overwrite=self.config.overwrite)

    def init_raster_tiler(self) -> RasterTiler:
        """
        Initialize the RasterTiler instance with configured tiles and properties.

        Args:
            None

        Returns:
            RasterTiler instance configured with tiles and properties

        Raises:
            WokflowManagerError: If initialization fails
            FileNotFoundError: If input directory doesn't exist
        """
        return RasterTiler(
            config=self.config,
        )

    def rasterize_all(
        self,
        overwrite: bool = True,
    ) -> bool:
        """
        Rasterize all tiles using the provided RasterTiler instance.

        Args:
            Tiler: RasterTiler instance configured with tiles and properties
            overwrite: Whether to overwrite existing raster files

        Returns:
            True if rasterization completed successfully

        Raises:
            WokflowManagerError: If rasterization fails
        """
        return self.raster_tiler.rasterize_all(overwrite=overwrite)

    def rasterize_vectors(self, paths, make_parents=True, overwrite=True) -> bool:
        """
        Rasterize vectors data for tiles.

        Args:
            paths: List of tile paths to rasterize vectors for
            make_parents: Whether to create parent directories if they don't exist
            overwrite: Whether to overwrite existing raster files

        Returns:
            True if vectors rasterization completed successfully

        Raises:
            WokflowManagerError: If vectors rasterization fails
        """
        return self.raster_tiler.rasterize_vectors(
            paths, make_parents=make_parents, overwrite=overwrite
        )

    def rasterize_vector(self, path: str, overwrite=True) -> bool:
        """
        Rasterize vector data for a single tile.

        Args:
            path: Tile path to rasterize vectors for
            overwrite: Whether to overwrite existing raster files

        Returns:
            True if vector rasterization completed successfully for the specified tile

        Raises:
            WokflowManagerError: If vector rasterization fails
        """
        return self.raster_tiler.rasterize_vectors(path, overwrite=overwrite)

    def run_3d_tiling(self) -> bool:
        """
        Run the 3D tile generation step of the workflow.

        Args:
            None

        Returns:
            True if tiling completed successfully

        Raises:
            WokflowManagerError: If tiling fails
            ValueError: If invalid tile format specified
        """
        self.cesium_3d_tiler = self.init_3d_tiling()

        self.cesium_3d_tiler.all_staged_to_3dtiles()
        self.cesium_3d_tiler.make_top_level_tileset()
        return True

    def init_3d_tiling(self) -> StagedTo3DConverter:
        """
        Initialize the StagedTo3DConverter instance for 3D tile generation.

        Args:
            None

        Returns:
            StagedTo3DConverter instance configured with tiles and properties

        Raises:
            WokflowManagerError: If initialization fails
            FileNotFoundError: If input directory doesn't exist
        """
        return StagedTo3DConverter(self.config)

    def generate_3d_tiles(self) -> bool:
        """
        Generate 3D tiles from the staged data.

        Args:
            None

        Returns:
            True if 3D tile generation completed successfully

        Raises:
            WokflowManagerError: If 3D tile generation fails
        """

        return self.cesium_3d_tiler.all_staged_to_3dtiles()

    def all_staged_to_3dtiles(self) -> None:
        """
        Process all staged vector tiles into 3D tiles.

        Args:
            None

        Returns:
            None
        """
        return self.cesium_3d_tiler.all_staged_to_3dtiles()

    def staged_to_3dtile(self, path: str) -> None:
        """
        Convert a staged vector tile into a B3DM tile file and a matching
        JSON tileset file.

        Parameters
        ----------
        path : str
            The path to the staged vector tile.

        Returns
        -------
        None
        """
        return self.cesium_3d_tiler.staged_to_3dtile(path)

    def parent_3d_tiles(self, tiles, bv_limit=None) -> None:
        """
        Get the parent directories of all 3D tiles.

        Returns
        -------
        None
        """
        return self.cesium_3d_tiler.parent_3dtiles_from_children(
            tiles, bv_limit=bv_limit
        )

    def make_top_level_tileset(self) -> None:
        """
        Create a top-level tileset for the 3D tiles.

        Returns
        -------
        None
        """
        return self.cesium_3d_tiler.make_top_level_tileset()

    def generate_wmts_capabilities(self) -> bool:
        """
        Generate WMTS capabilities document.

        Args:
            None

        Returns:
            True if capabilities generation completed successfully

        Raises:
            WokflowManagerError: If capabilities generation fails
        """
        pass

    def run_workflow(self) -> None:
        """
        Run the complete visualization workflow.

        This method orchestrates all workflow steps in the correct order:
        1. Data staging
        2. Rasterization
        3. Tile generation
        4. 3D conversion (if configured)
        5. WMTS capabilities generation

        Args:
            None

        Returns:
            Dictionary with success status for each workflow step

        Raises:
            WokflowManagerError: If workflow fails
        """
        pass


# CLI interface
@click.group()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True),
    help="Configuration file path",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    default="INFO",
    help="Logging level",
)
@click.pass_context
def cli(ctx: click.Context, config: Optional[str], log_level: str) -> None:
    """PDG Visualization Workflow Manager CLI."""


if __name__ == "__main__":
    cli()
