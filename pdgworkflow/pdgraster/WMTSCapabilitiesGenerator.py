import xml.etree.ElementTree as ET
from xml.dom import minidom

LEFT_BOUNDS_LIMIT = -179.999999
RIGHT_BOUNDS_LIMIT = 179.999999
DEFAULT_BOUNDS = {
            "left": LEFT_BOUNDS_LIMIT,
            "right": RIGHT_BOUNDS_LIMIT,
            "bottom": -90,
            "top": 90
        }


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
        tile_matrix_set : str
        tile_width : int
        tile_height : int
        max_z_level : int
        bounding_box : dict, optional
            A dictionary with keys 'left', 'right', 'bottom', and 'top' that
            specify the bounding box of the raster. If set to None, the total
            bounds of the map (global extent) are used:
            {'left': -179.999999, 'right': 179.999999, 'bottom': -90, 'top': 90}.

    Usage Example:
        generator = WMTSCapabilitiesGenerator(
            title="PDG Ice-wedge polygon high",
            base_url="https://arcticdata.io/data/tiles",
            doi="10.18739/A2KW57K57",
            layer_title="iwp_high",
            layer_identifier="iwp_high",
            tile_format=".png",
            tile_matrix_set="WGS1984Quad",
            tile_width=256,
            tile_height=256,
            max_z_level=15,
            bounding_box={
                "left": -179.91531896747117,
                "right": 179.91531896747247,
                "bottom": 50.16996707215903,
                "top": 80.0978646943821
            }
        )
        xml_str = generator.generate_capabilities()
    """
    # Class-level constants and schema
    XMLNS = {
        "default": "http://www.opengis.net/wmts/1.0",
        "ows": "http://www.opengis.net/ows/1.1",
        "xlink": "http://www.w3.org/1999/xlink",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "gml": "http://www.opengis.net/gml"
    }
    SCHEMA_LOCATION = (
        "http://www.opengis.net/wmts/1.0 http://schemas.opengis.net/wmts/1.0/wmtsGetCapabilities_response.xsd"
    )

    # Mapping of file extension
    EXTENSION_MAPPING: dict[str, str] = {
        ".png": "image/png",
        ".jpeg": "image/jpeg",
        ".jpg": "image/jpg",
        ".tiff": "image/tiff",
        ".kmz": "application/vnd.google-earth.kmz+xml",
        ".kmz;jpeg": "application/vnd.google-earth.kmz+xml;image_type=image/jpeg",
        ".kmz;png": "application/vnd.google-earth.kmz+xml;image_type=image/png",
        ".shp": "application/x-esri-shape",
        ".json": "application/json",
        ".tiff;8": "image/tiff;depth=8",
        ".tiff;16": "image/tiff;depth=16",
        ".tiff;32f": "image/tiff;depth=32f"
    }

    SCALE_DENOMINATORS: list[float] = [
        279541132.0143589,
        139770566.0071794,
        69885283.00358972,
        34942641.50179486,
        17471320.75089743,
        8735660.375448715,
        4367830.187724357,
        2183915.093862179,
        1091957.546931089,
        545978.7734655447,
        272989.3867327723,
        136494.6933663862,
        68247.34668319309,
        34123.67334159654,
        17061.83667079827,
        8530.918335399136,
        4265.459167699568,
        2132.729583849784
    ]

    TOP_LEFT_CORNER: str = "-180 90"
    

    def __init__(
        self,
        title: str,
        base_url: str,
        doi: str,
        layer_title: str,
        layer_identifier: str,
        tile_format: str,
        tile_matrix_set: str,
        tile_width: int,
        tile_height: int,
        max_z_level: int = 13,
        bounding_box: dict = None
    ):
        
        """
        The following parameters should contain in the config.json file
        """
        self.title = title
        self.base_url = base_url
        self.doi = doi
        self.layer_title = layer_title
        self.layer_identifier = layer_identifier
        self.tile_format = tile_format
        self.tile_matrix_set = tile_matrix_set
        self.tile_width = tile_width
        self.tile_height = tile_height
        self.max_z_level = max_z_level

        self.capabilities_url = f"{base_url}/{doi}/WMTSCapabilities.xml"
        self.tiles_url = f"{base_url}/{doi}/"
        self.bounding_box = bounding_box or DEFAULT_BOUNDS

        # Configure resource template based on tile_format
        self.resource_template = self._configure_resource_template()


        if not (0 <= max_z_level <= 13):
            raise ValueError(f"max_z_level must be between 0 and 13.")

        self.bounding_box["left"] = max(self.bounding_box["left"], LEFT_BOUNDS_LIMIT)
        self.bounding_box["right"] = min(self.bounding_box["right"], RIGHT_BOUNDS_LIMIT)


    def _configure_resource_template(self) -> str:
        if self.tile_format not in self.EXTENSION_MAPPING:
            raise ValueError(f"Unsupported tile format: {self.tile_format}")
        return f"{self.base_url}/{self.doi}/{{TileMatrixSet}}/{{TileMatrix}}/{{TileCol}}/{{TileRow}}{self.tile_format}"



    def generate_capabilities(self) -> str:
        """
        Generates the WMTS Capabilities XML as a formatted string.
        
        Returns:
            An XML string representing the WMTS Capabilities document.
        """
        root = ET.Element("Capabilities", attrib={
            "xmlns": WMTSCapabilitiesGenerator.XMLNS["default"],
            "xmlns:ows": WMTSCapabilitiesGenerator.XMLNS["ows"],
            "xmlns:xlink": WMTSCapabilitiesGenerator.XMLNS["xlink"],
            "xmlns:xsi": WMTSCapabilitiesGenerator.XMLNS["xsi"],
            "xmlns:gml": WMTSCapabilitiesGenerator.XMLNS["gml"],
            "xsi:schemaLocation": WMTSCapabilitiesGenerator.SCHEMA_LOCATION,
            "version": "1.0.0"
        })


        self._add_service_identification(root)
        self._add_operations_metadata(root)
        self._add_contents(root)
        ET.SubElement(root, "ServiceMetadataURL", attrib={"xlink:href": self.capabilities_url})

        xml_bytes = ET.tostring(root, encoding="utf-8")
        parsed_xml = minidom.parseString(xml_bytes)

        # Return the XML string with UTF-8 encoding
        return parsed_xml.toprettyxml(indent="  ", encoding="UTF-8").decode("utf-8")
    

    def _add_service_identification(self, root):
        service_identification = ET.SubElement(root, "ows:ServiceIdentification")
        ET.SubElement(service_identification, "ows:Title").text = self.title
        ET.SubElement(service_identification, "ows:ServiceType").text = "OGC WMTS"
        ET.SubElement(service_identification, "ows:ServiceTypeVersion").text = "1.0.0"

    def _add_operations_metadata(self, root):
        operations_metadata = ET.SubElement(root, "ows:OperationsMetadata")
        self._add_operation(operations_metadata, "GetCapabilities", self.capabilities_url)
        self._add_operation(operations_metadata, "GetTile", self.tiles_url)

    def _add_operation(self, parent, name, href):
        operation = ET.SubElement(parent, "ows:Operation", attrib={"name": name})
        dcp = ET.SubElement(operation, "ows:DCP")
        http = ET.SubElement(dcp, "ows:HTTP")
        get = ET.SubElement(http, "ows:Get", attrib={"xlink:href": href})
        constraint = ET.SubElement(get, "ows:Constraint", attrib={"name": "GetEncoding"})
        allowed_values = ET.SubElement(constraint, "ows:AllowedValues")
        ET.SubElement(allowed_values, "ows:Value").text = "RESTful"

    def _add_contents(self, root):
        contents = ET.SubElement(root, "Contents")
        layer = ET.SubElement(contents, "Layer")
        ET.SubElement(layer, "ows:Title").text = "iwp_high"
        ET.SubElement(layer, "ows:Identifier").text = "iwp_high"

        wgs84_bbox = ET.SubElement(layer, "ows:WGS84BoundingBox")
        ET.SubElement(wgs84_bbox, "ows:LowerCorner").text = f"{self.bounding_box['left']} {self.bounding_box['bottom']}"
        ET.SubElement(wgs84_bbox, "ows:UpperCorner").text = f"{self.bounding_box['right']} {self.bounding_box['top']}"


        style = ET.SubElement(layer, "Style", attrib={"isDefault": "true"})
        ET.SubElement(style, "ows:Title").text = "Default Style"
        ET.SubElement(style, "ows:Identifier").text = "default"

        ET.SubElement(layer, "Format").text = WMTSCapabilitiesGenerator.EXTENSION_MAPPING[self.tile_format]
        tile_matrix_set_link = ET.SubElement(layer, "TileMatrixSetLink")
        ET.SubElement(tile_matrix_set_link, "TileMatrixSet").text = "WGS1984Quad"

        ET.SubElement(layer, "ResourceURL", attrib={
            "format": WMTSCapabilitiesGenerator.EXTENSION_MAPPING[self.tile_format],
            "resourceType": "tile",
            "template": self.resource_template
        })

        self._add_tile_matrix_set(contents)

    def _add_tile_matrix_set(self, contents: ET.Element):
        tile_matrix_set = ET.SubElement(contents, "TileMatrixSet", attrib={"xml:id": "WorldCRS84Quad"})
        ET.SubElement(tile_matrix_set, "ows:Title").text = "CRS84 for the World"
        ET.SubElement(tile_matrix_set, "ows:Identifier").text = "WGS1984Quad"

        bounding_box = ET.SubElement(tile_matrix_set, "ows:BoundingBox", attrib={"crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"})
        ET.SubElement(bounding_box, "ows:LowerCorner").text = f"{self.bounding_box['left']} {self.bounding_box['bottom']}"
        ET.SubElement(bounding_box, "ows:UpperCorner").text = f"{self.bounding_box['right']} {self.bounding_box['top']}"

        ET.SubElement(tile_matrix_set, "ows:SupportedCRS").text = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
        ET.SubElement(tile_matrix_set, "WellKnownScaleSet").text = "http://www.opengis.net/def/wkss/OGC/1.0/GoogleCRS84Quad"

        for i in range(self.max_z_level + 1):  # Generate levels from 0 to max_z_level
            tile_matrix = ET.SubElement(tile_matrix_set, "TileMatrix")

            scale_denominator = WMTSCapabilitiesGenerator.SCALE_DENOMINATORS[i]  

            ET.SubElement(tile_matrix, "ows:Identifier").text = str(i)
            ET.SubElement(tile_matrix, "ScaleDenominator").text = str(scale_denominator)
            ET.SubElement(tile_matrix, "TopLeftCorner").text = WMTSCapabilitiesGenerator.TOP_LEFT_CORNER
            ET.SubElement(tile_matrix, "TileWidth").text = str(self.tile_width)
            ET.SubElement(tile_matrix, "TileHeight").text = str(self.tile_height)
            ET.SubElement(tile_matrix, "MatrixWidth").text = str(2 ** (i+1))
            ET.SubElement(tile_matrix, "MatrixHeight").text = str(2 ** i)