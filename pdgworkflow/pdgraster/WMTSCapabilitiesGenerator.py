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

    generator = WMTSCapabilitiesGenerator(
            title="This is the title",
            base_url="https://example.com",
            doi="doi:10.5066/F7VQ30RM",
            layer_title="layer_title",
            layer_identifier="layer_identifier",
            bounding_box=None,
            tile_format=".png",
            tile_matrix_set_id="WorldCRS84Quad",
            max_z_level=3
        )

    wmts_xml = generator.generate_capabilities()
    with open("WMTSCapabilities.xml", "w") as f:
        f.write(wmts_xml)
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

    def __init__(
        self,
        title: str,
        base_url: str,
        doi: str,
        layer_title: str,
        layer_identifier: str,
        tile_format: str,
        tile_matrix_set_id: str,
        max_z_level: int,
        bounding_box: dict = None
    ):
        
        self.title = title
        self.base_url = base_url
        self.doi = doi
        self.layer_title = layer_title
        self.layer_identifier = layer_identifier
        self.tile_format = tile_format
        self.tile_matrix_set_id = tile_matrix_set_id
        self.max_z_level = max_z_level

        self.capabilities_url = f"{base_url}/{doi}/WMTSCapabilities.xml"
        self.tiles_url = f"{base_url}/{doi}/"

        # Configure resource template based on tile_format
        self.resource_template = self._configure_resource_template()

        # Get the TileMatrixSet Object from morecantile
        self.tms_object = morecantile.tms.get(self.tile_matrix_set_id)
        self.tms_bounding_box = self.tms_object.xy_bbox
        self.top_left_corner = f"{self.tms_bounding_box.left} {self.tms_bounding_box.top}"
        self.bounding_box = bounding_box or self.tms_bounding_box
        self.crs = self.tms_object.crs.root
        self.wellKnownScaleSet = self.tms_object.wellKnownScaleSet

        max_tms_zoom = self.tms_object.maxzoom
        if not (0 <= max_z_level <= max_tms_zoom):
            raise ValueError(f"max_z_level must be between 0 and {max_tms_zoom}.")

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
            "xmlns": self.XMLNS["default"],
            "xmlns:ows": self.XMLNS["ows"],
            "xmlns:xlink": self.XMLNS["xlink"],
            "xmlns:xsi": self.XMLNS["xsi"],
            "xmlns:gml": self.XMLNS["gml"],
            "xsi:schemaLocation": self.SCHEMA_LOCATION,
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
        ET.SubElement(service_identification, "ows:Title").text = f"{self.title}"
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
        ET.SubElement(layer, "ows:Title").text = self.layer_title
        ET.SubElement(layer, "ows:Identifier").text = self.layer_identifier

        wgs84_bbox = ET.SubElement(layer, "ows:WGS84BoundingBox")
        ET.SubElement(wgs84_bbox, "ows:LowerCorner").text = f"{self.bounding_box.left} {self.bounding_box.bottom}"
        ET.SubElement(wgs84_bbox, "ows:UpperCorner").text = f"{self.bounding_box.right} {self.bounding_box.top}"

        style = ET.SubElement(layer, "Style", attrib={"isDefault": "true"})
        ET.SubElement(style, "ows:Title").text = "Default Style"
        ET.SubElement(style, "ows:Identifier").text = "default"

        ET.SubElement(layer, "Format").text = self.EXTENSION_MAPPING[self.tile_format]
        tile_matrix_set = ET.SubElement(layer, "TileMatrixSetLink")
        ET.SubElement(tile_matrix_set, "TileMatrixSet").text = self.tile_matrix_set_id

        ET.SubElement(layer, "ResourceURL", attrib={
            "format": self.EXTENSION_MAPPING[self.tile_format],
            "resourceType": "tile",
            "template": self.resource_template
        })

        self._add_tile_matrix_set(contents)

    def _add_tile_matrix_set(self, contents: ET.Element):
        tile_matrix_set = ET.SubElement(contents, "TileMatrixSet", attrib={"xml:id": self.tile_matrix_set_id})
        ET.SubElement(tile_matrix_set, "ows:Title").text = self.tms_object.title
        ET.SubElement(tile_matrix_set, "ows:Identifier").text = self.tile_matrix_set_id

        b_box = ET.SubElement(tile_matrix_set, "ows:BoundingBox", attrib={"crs": self.crs})
        ET.SubElement(b_box, "ows:LowerCorner").text = f"{self.tms_bounding_box.left} {self.tms_bounding_box.bottom}"
        ET.SubElement(b_box, "ows:UpperCorner").text = f"{self.tms_bounding_box.right} {self.tms_bounding_box.top}"

        ET.SubElement(tile_matrix_set, "ows:SupportedCRS").text = f"{self.crs}"
        ET.SubElement(tile_matrix_set, "WellKnownScaleSet").text = f"{self.wellKnownScaleSet}"

        for i in range(self.max_z_level + 1):  # Generate levels from 0 to max_z_level
            tile_matrix = ET.SubElement(tile_matrix_set, "TileMatrix")
           
            scale_denominator =  self.tms_object.matrix(i).scaleDenominator
            tile_width = self.tms_object.matrix(i).tileWidth
            tile_height = self.tms_object.matrix(i).tileHeight

            ET.SubElement(tile_matrix, "ows:Identifier").text = str(i)
            ET.SubElement(tile_matrix, "ScaleDenominator").text = str(scale_denominator)
            ET.SubElement(tile_matrix, "TopLeftCorner").text = self.top_left_corner
            ET.SubElement(tile_matrix, "TileWidth").text = str(tile_width)
            ET.SubElement(tile_matrix, "TileHeight").text = str(tile_height)
            ET.SubElement(tile_matrix, "MatrixWidth").text = str(2 ** (i+1))
            ET.SubElement(tile_matrix, "MatrixHeight").text = str(2 ** i)