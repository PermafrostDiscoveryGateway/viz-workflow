import xml.etree.ElementTree as ET
import morecantile
from xml.dom import minidom


class WMTSCapabilitiesGenerator:
    """
    A class to generate WMTS Capabilities XML for a given dataset.

    Parameters:
        title : str
        base_url : str
        doi : str
        layer_title : str
        layer_identifier : str
        tile_format : str (e.g., '.png').
        tile_matrix_set_id : str
        max_z_level : int
        bounding_box : dict, optional
            A dictionary with keys 'left', 'right', 'bottom', and 'top' that
            specify the bounding box of the raster. If set to None, the total
            bounds of the map (global extent) are used.

    Usage Example:
    1. document-level settings

    ice_wedge_polygons_wmts = WMTSCapabilitiesGenerator(
        title="Ice-Wedge Polygons",
        base_url="https://arcticdata.io/data",
        doi="10.18739/A2KW57K57",
        tile_matrix_set_id="WGS1984Quad",
        max_z_level=15,
        bounding_box={"left":-179.0, "bottom":-87.0, "right":176.0, "top":90.0},
    )

    2. Add layers: PNG
        ice_wedge_polygons_wmts.add_layer(
            layer_title="Ice-Wedge Polygons (png)",
            layer_identifier="iwp_png",
            tile_format=".png",
        )

    3. Add layers: GeoTiff
        ice_wedge_polygons_wmts.add_layer(
            layer_title="Ice-Wedge Polygons (GeoTiff)",
            layer_identifier="iwp_geotiff",
            tile_format=".tiff",
            url_postfix="iwp_geotiff_high",
        )

    4. Add layers: GeoPackage

    ice_wedge_polygons_wmts.add_layer(
        layer_title="Ice-Wedge Polygons (GeoPackage)",
        layer_identifier="iwp_geopackage",
        tile_format=".gpkg",
        url_postfix="iwp_geopackage_high",
    )

    5. write
        xml_text = ice_wedge_polygons_wmts.generate_capabilities()

        with open("WMTSCapabilities.xml", "w", encoding="utf-8") as f:
            f.write(xml_text)
    """

    # Class-level constants and schema
    XMLNS = {
        "default": "http://www.opengis.net/wmts/1.0",
        "ows": "http://www.opengis.net/ows/1.1",
        "xlink": "http://www.w3.org/1999/xlink",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "gml": "http://www.opengis.net/gml",
    }
    SCHEMA_LOCATION = "http://www.opengis.net/wmts/1.0 http://schemas.opengis.net/wmts/1.0/wmtsGetCapabilities_response.xsd"

    # Mapping of file extension
    EXTENSION_MAPPING: dict[str, str] = {
        ".png": "image/png",
        ".jpeg": "image/jpeg",
        ".jpg": "image/jpg",
        ".tiff": "image/geotiff",
        ".tif": "image/geotiff",
        ".kmz": "application/vnd.google-earth.kmz+xml",
        ".kmz;jpeg": "application/vnd.google-earth.kmz+xml;image_type=image/jpeg",
        ".kmz;png": "application/vnd.google-earth.kmz+xml;image_type=image/png",
        ".shp": "application/x-esri-shape",
        ".json": "application/json",
        ".gpkg": "application/geopackage",
        ".tiff;8": "image/tiff;depth=8",
        ".tiff;16": "image/tiff;depth=16",
        ".tiff;32f": "image/tiff;depth=32f",
    }

    def __init__(
        self,
        title: str,
        base_url: str,
        doi: str,
        tile_matrix_set_id: str,
        max_z_level: int,
        bounding_box: dict | None = None,
    ):

        self.title = title
        self.base_url = base_url
        self.doi = doi
        self.tile_matrix_set_id = tile_matrix_set_id
        self.max_z_level = max_z_level

        self.capabilities_url = f"{base_url}/{doi}/WMTSCapabilities.xml"
        self.tiles_url = f"{base_url}/tiles/{doi}/"

        # Get the TileMatrixSet Object from morecantile
        self.tms_object = morecantile.tms.get(self.tile_matrix_set_id)
        self.top_left_corner = f"{self.tms_object.bbox.left} {self.tms_object.bbox.top}"

        if bounding_box == None:
            self.bounding_box = {
                "left": self.tms_object.bbox.left,
                "bottom": self.tms_object.bbox.bottom,
                "right": self.tms_object.bbox.right,
                "top": self.tms_object.bbox.top,
            }
        else:
            self.bounding_box = bounding_box

        self.crs = "self.tms_object.crs.root"
        self.wellKnownScaleSet = self.tms_object.wellKnownScaleSet

        max_tms_zoom = self.tms_object.maxzoom
        if not (0 <= max_z_level <= max_tms_zoom):
            raise ValueError(f"max_z_level must be between 0 and {max_tms_zoom}.")

        self._layers: list[dict] = []

    def add_layer(
        self,
        *,
        layer_title: str,
        layer_identifier: str,
        tile_format: str,
        url_postfix: str = "",
    ) -> None:

        if tile_format not in self.EXTENSION_MAPPING:
            raise ValueError(f"Unsupported tile format: {tile_format}")
        if tile_format == ".png":
            prefix = f"{self.base_url}/tiles/{self.doi}"
        else:
            prefix = f"{self.base_url}/{self.doi}"

        template = (
            f"{prefix}/"
            "{TileMatrixSet}/{TileMatrix}/{TileCol}/{TileRow}"
            f"{tile_format}"
        )

        self._layers.append(
            {
                "title": layer_title,
                "identifier": layer_identifier,
                "tile_format": tile_format,
                "mime": self.EXTENSION_MAPPING[tile_format],
                "template": template,
            }
        )

    def generate_capabilities(self) -> str:
        if not self._layers:
            raise ValueError("No layers declared. Call add_layer() first.")

        root = ET.Element(
            "Capabilities",
            attrib={
                "xmlns": self.XMLNS["default"],
                "xmlns:ows": self.XMLNS["ows"],
                "xmlns:xlink": self.XMLNS["xlink"],
                "xmlns:xsi": self.XMLNS["xsi"],
                "xmlns:gml": self.XMLNS["gml"],
                "xsi:schemaLocation": self.SCHEMA_LOCATION,
                "version": "1.0.0",
            },
        )

        self._add_service_identification(root)
        self._add_operations_metadata(root)
        self._add_contents(root)
        ET.SubElement(
            root, "ServiceMetadataURL", attrib={"xlink:href": self.capabilities_url}
        )

        xml_bytes = ET.tostring(root, encoding="utf-8")
        parsed_xml = minidom.parseString(xml_bytes)

        # Return the XML string with UTF-8 encoding
        return parsed_xml.toprettyxml(indent="  ", encoding="UTF-8").decode("utf-8")

    def _add_service_identification(self, root):
        service_identification = ET.SubElement(root, "ows:ServiceIdentification")
        ET.SubElement(service_identification, "ows:Title").text = f"{self.title}"
        ET.SubElement(service_identification, "ows:ServiceType").text = "OGC WMTS"
        ET.SubElement(service_identification, "ows:ServiceTypeVersion").text = "1.0.0"

    def _add_operations_metadata(self, root):
        operations_metadata = ET.SubElement(root, "ows:OperationsMetadata")
        self._add_operation(
            operations_metadata, "GetCapabilities", self.capabilities_url
        )
        self._add_operation(operations_metadata, "GetTile", self.tiles_url)

    def _add_operation(self, parent, name, href):
        operation = ET.SubElement(parent, "ows:Operation", attrib={"name": name})
        dcp = ET.SubElement(operation, "ows:DCP")
        http = ET.SubElement(dcp, "ows:HTTP")
        get = ET.SubElement(http, "ows:Get", attrib={"xlink:href": href})
        constraint = ET.SubElement(
            get, "ows:Constraint", attrib={"name": "GetEncoding"}
        )
        allowed_values = ET.SubElement(constraint, "ows:AllowedValues")
        ET.SubElement(allowed_values, "ows:Value").text = "RESTful"

    def _add_contents(self, root):
        contents = ET.SubElement(root, "Contents")
        for lyr in self._layers:
            layer_el = ET.SubElement(contents, "Layer")
            ET.SubElement(layer_el, "ows:Title").text = lyr["title"]
            ET.SubElement(layer_el, "ows:Identifier").text = lyr["identifier"]

            wgs84_bbox = ET.SubElement(layer_el, "ows:WGS84BoundingBox")
            ET.SubElement(wgs84_bbox, "ows:LowerCorner").text = (
                f'{self.bounding_box["left"]} {self.bounding_box["bottom"]}'
            )
            ET.SubElement(wgs84_bbox, "ows:UpperCorner").text = (
                f'{self.bounding_box["right"]} {self.bounding_box["top"]}'
            )

            style = ET.SubElement(layer_el, "Style", isDefault="true")
            ET.SubElement(style, "ows:Title").text = "Default Style"
            ET.SubElement(style, "ows:Identifier").text = "default"

            ET.SubElement(layer_el, "Format").text = lyr["mime"]
            tms_link = ET.SubElement(layer_el, "TileMatrixSetLink")
            ET.SubElement(tms_link, "TileMatrixSet").text = self.tile_matrix_set_id

            ET.SubElement(
                layer_el,
                "ResourceURL",
                format=lyr["mime"],
                resourceType="tile",
                template=lyr["template"],
            )
        self._add_tile_matrix_set(contents)

    def _add_tile_matrix_set(self, contents: ET.Element):
        tile_matrix_set = ET.SubElement(contents, "TileMatrixSet")
        ET.SubElement(tile_matrix_set, "ows:Title").text = self.tms_object.title
        ET.SubElement(tile_matrix_set, "ows:Identifier").text = self.tile_matrix_set_id

        b_box = ET.SubElement(
            tile_matrix_set, "ows:BoundingBox", attrib={"crs": self.crs}
        )
        ET.SubElement(b_box, "ows:LowerCorner").text = (
            f"{self.tms_object.bbox.left} {self.tms_object.bbox.bottom}"
        )
        ET.SubElement(b_box, "ows:UpperCorner").text = (
            f"{self.tms_object.bbox.right} {self.tms_object.bbox.top}"
        )

        ET.SubElement(tile_matrix_set, "ows:SupportedCRS").text = f"{self.crs}"

        # Adding WellKnownScaleSet only if it is defined
        if self.wellKnownScaleSet:
            ET.SubElement(tile_matrix_set, "WellKnownScaleSet").text = (
                f"{self.wellKnownScaleSet}"
            )

        for i in range(self.max_z_level + 1):  # Generate levels from 0 to max_z_level
            tile_matrix = ET.SubElement(tile_matrix_set, "TileMatrix")

            scale_denominator = self.tms_object.matrix(i).scaleDenominator
            tile_width = self.tms_object.matrix(i).tileWidth
            tile_height = self.tms_object.matrix(i).tileHeight

            ET.SubElement(tile_matrix, "ows:Identifier").text = str(i)
            ET.SubElement(tile_matrix, "ScaleDenominator").text = str(scale_denominator)
            ET.SubElement(tile_matrix, "TopLeftCorner").text = self.top_left_corner
            ET.SubElement(tile_matrix, "TileWidth").text = str(tile_width)
            ET.SubElement(tile_matrix, "TileHeight").text = str(tile_height)
            ET.SubElement(tile_matrix, "MatrixWidth").text = str(2 ** (i + 1))
            ET.SubElement(tile_matrix, "MatrixHeight").text = str(2**i)
