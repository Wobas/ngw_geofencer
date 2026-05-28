import os
import requests
import json
import time
from dotenv import load_dotenv

load_dotenv()
BASE_URL = os.getenv("NGW_HOST", "https://sandbox.nextgis.com")
USERNAME = os.getenv("NGW_LOGIN", "administrator")
PASSWORD = os.getenv("NGW_PASSWORD", "demodemo")
GEOMETRY_MODIFIER_INTERVAL_SEC = int(os.getenv("GEOMETRY_MODIFIER_INTERVAL_SEC", "5"))

PARENT_ID = 0
GROUP_KEYNAME = "resource_group_geofencing"
TOP_LAYER_KEYNAME = "top_layer_geofencing"
BOTTOM_LAYER_KEYNAME = "bottom_layer_geofencing"

GROUP_PAYLOAD = {
    "resource": {
        "cls": "resource_group",
        "parent": {"id": PARENT_ID},
        "keyname": GROUP_KEYNAME,
        "display_name": "Group with changing geometry"
    }
}

POINT_LAYER_PAYLOAD = {
    "resource": {
        "cls": "vector_layer",
        "keyname": TOP_LAYER_KEYNAME,
        "display_name": "Point Layer"
    },
    "vector_layer": {
        "geometry_type": "POINT",
        "srs": {"id": 4326},
        "fields": []
    },
    "versioning": {
        "enabled": True
    }
}

POLYGON_LAYER_PAYLOAD = {
    "resource": {
        "cls": "vector_layer",
        "keyname": BOTTOM_LAYER_KEYNAME,
        "display_name": "Polygon Layer"
    },
    "vector_layer": {
        "geometry_type": "POLYGON",
        "srs": {"id": 4326},
        "fields": []
    },
    "versioning": {
        "enabled": True
    }
}

INITIAL_POINT_GEOM = "POINT (0.0 0.0)"
INITIAL_POLYGON_GEOM = "POLYGON ((10.0 30.0, 40.0 40.0, 40.0 0.0, 20.0 10.0, 10.0 30.0))"

def create_resource(payload: dict) -> int:
    """
    Create resource based on dict with payload.
    
    Parameters
    ---------
    payload : dict
        payload information to create empty resource with specified parent, keyname and display name

    Returns
    -------
    int
        ID of the created resource for the current web-gis
    """
    url = f"{BASE_URL}/api/resource/"
    r = requests.post(
        url,
        auth=(USERNAME, PASSWORD),
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload)
    )
    r.raise_for_status()
    return r.json()["id"]

def create_feature(layer_id: int, geom: str) -> dict[int, int]:
    """
    Create feature in IFeatureLayer based on dict with payload.
    
    Parameters
    ---------
    layer_id : int
        ID of the parent IFeatureLayer
    
    geom : str
        string with geometry of the feature in the WKT format

    Returns
    -------
    dict[int, int]
        returns the dict with id of created feature and version of the IFeatureLayer
    """
    url = f"{BASE_URL}/api/resource/{layer_id}/feature/?srs=4326"
    payload = {
        "geom": geom,
        "fields": {}
    }
    r = requests.post(
        url,
        auth=(USERNAME, PASSWORD),
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload)
    )
    r.raise_for_status()
    return r.json()

def search_resource(resource_keyname: str) -> int|None:
    """
    Search resource by a keyname.
    
    Parameters
    ---------
    resource_keyname : str
        keyname of the target resource

    Returns
    -------
    int | None
        return contains None if the resource wasn't found, and int with them ID if the resource was found
    """
    url = f"{BASE_URL}/api/resource/search/?keyname={resource_keyname}"
    r = requests.get(
        url,
        auth=(USERNAME, PASSWORD)
    )
    if r.status_code == 200 and len(r.json()) > 0:
        return r.json()[0]['resource']['id']
    else: 
        return None

def get_resource_id() -> int:
    """
    Search resource by a default keyname, if not found, it creates.

    Returns
    -------
    int
        return int ID
    """
    group_id = search_resource(GROUP_KEYNAME)
    if (not group_id):
        group_id = create_resource(GROUP_PAYLOAD)
        print("Group created:", group_id)
    return group_id

def get_layer_id(parent_id: int, layer_keyname: str, layer_payload: dict, geom: str) -> int:
    """
    Search layer by a keyname, if not found, it creates.
    
    Parameters
    ---------
    parent_id : int
        id of the parent to create layer in the specified directory
    layer_keyname : str
        keyname of the target layer
    layer_payload : dict
        payload information to create empty layer with specified parent, keyname, display name, geometry type and srs
    geom : str
        string with geometry in WKT format

    Returns
    -------
    int | None
        return contains None if the resource wasn't found, and int with them ID if the resource was found
    """
    layer_id = search_resource(layer_keyname)
    if (not layer_id):
        layer_payload = layer_payload
        layer_payload["resource"]["parent"] = {"id": parent_id}

        layer_id = create_resource(layer_payload)
        print(f"Created {layer_payload['vector_layer']['geometry_type']} layer:", layer_id)
        
        create_feature(layer_id, geom)
    return layer_id


if __name__ == '__main__':
    group_id = get_resource_id()
    point_layer_id = get_layer_id(group_id, TOP_LAYER_KEYNAME, POINT_LAYER_PAYLOAD, INITIAL_POINT_GEOM)

    url = f"{BASE_URL}/api/resource/{point_layer_id}/feature/1"
    while True:
        for i in range(5):
            for j in range(5):
                payload = {
                    "geom": f"POINT ({j*10} {i*10})"
                }

                response = requests.put(
                    url,
                    auth=(USERNAME, PASSWORD),
                    headers={"Content-Type": "application/json"},
                    data=json.dumps(payload)
                )

                if response.status_code == 200:
                    print("Геометрия успешно обновлена")
                else:
                    print("Ошибка:", response.status_code, response.text)

                time.sleep(GEOMETRY_MODIFIER_INTERVAL_SEC)
