import json
from typing import Optional, List, Dict
import morecantile
import re

class STACGenerator:

    def __init__(
        self,
        title: str,
        base_url: str,
        doi: str,
        tile_matrix_set_id: str,
        max_z_level: int,
        bounding_box: Optional[dict] = None,
    ):
        self.title = title
        self.base_url = base_url.rstrip("/")
        self.doi = doi
        self.tile_matrix_set_id = tile_matrix_set_id
        self.max_z_level = max_z_level

        self.tms_object = morecantile.tms.get(self.tile_matrix_set_id)

        if bounding_box == None:
            self.bounding_box = {
                "left": self.tms_object.bbox.left,
                "bottom": self.tms_object.bbox.bottom,
                "right": self.tms_object.bbox.right,
                "top": self.tms_object.bbox.top,
            }
        else:
            self.bounding_box = bounding_box

        self.assets: Dict[str, dict] = {}

    def add_asset(
        self,
        key: str,
        title: str,
        href: str,
        media_type: str,
        roles: Optional[List[str]] = None,
    ) -> None:
        self.assets[key] = {
            "title": title,
            "href": href,
            "type": media_type,
            "roles": roles or ["data"],
        }

    def _with_tms_in_tiles(self, assets: Dict[str, dict]) -> Dict[str, dict]:
        out = {}
        for k, a in assets.items():
            a_copy = dict(a)
            roles = a_copy.get("roles") or []
            href = a_copy.get("href", "")
            if "tiles" in roles and "{z}" in href and self.tile_matrix_set_id not in href:
                a_copy["href"] = re.sub(
                    r"/(\{z\}/)",
                    f"/{self.tile_matrix_set_id}/" + r"\1",
                    href,
                    count=1,
                )
            out[k] = a_copy
        return out

    def build_collection_obj(self, parent_catalog_href: Optional[str] = None) -> dict:
        collection = {
            "stac_version": "1.0.0",
            "type": "Collection",
            "id": self.doi,
            "description": self.title,
            "title": self.title,
            "license": "CC-BY-4.0",
            "extent": {
                "spatial": {"bbox": [self.bounding_box]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [],
            "assets": self._with_tms_in_tiles(self.assets),
            "stac_extensions": [],
        }

        if parent_catalog_href:
            collection["links"] = [
                {
                    "rel": "parent",
                    "href": parent_catalog_href,
                    "type": "application/json",
                    "title": "Arctic Data Root",
                }
            ]
        else:
            collection["links"] = [
                {
                    "rel": "self",
                    "href": f"{self.base_url}/{self.doi}/collection.json",
                }
            ]

        return collection

    def generate_collection(self) -> str:
        return json.dumps(self.build_collection_obj(), indent=2)

    def generate_catalog(
        self,
        catalog_id: str,
        catalog_description: str,
    ) -> str:
        
        catalog_self_href = f"{self.base_url}/catalog.json"

        catalog_obj = {
            "stac_version": "1.0.0",
            "type": "Catalog",
            "id": catalog_id,
            "description": catalog_description,
            "links": [
                {
                    "rel": "self",
                    "href": catalog_self_href,
                }
            ],
            "children": [
                self.build_collection_obj(parent_catalog_href=catalog_self_href)
            ],
        }
        return json.dumps(catalog_obj, indent=2)