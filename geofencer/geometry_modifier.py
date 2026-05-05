import requests
import json
import time

BASE_URL = "https://sandbox.nextgis.com"
USERNAME = "administrator"
PASSWORD = "demodemo"

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


def create_resource(payload:dict) -> str:
    url = f"{BASE_URL}/api/resource/"
    r = requests.post(
        url,
        auth=(USERNAME, PASSWORD),
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload)
    )
    r.raise_for_status()
    return r.json()["id"]

def create_feature(layer_id:str, geom:str):
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

def search_resource(resource_keyname) -> str|None:
    url = f"{BASE_URL}/api/resource/search/?keyname={resource_keyname}"
    r = requests.get(
        url,
        auth=(USERNAME, PASSWORD)
    )
    if r.status_code == 200 and len(r.json()) > 0:
        return r.json()[0]['resource']['id']
    else: 
        return None

def get_resource_id() -> str:
    group_id = search_resource(GROUP_KEYNAME)
    if (not group_id):
        group_id = create_resource(GROUP_PAYLOAD)
        print("Создана группа:", group_id)
    return group_id

def get_point_layer_id(parent_id:str) -> str:
    point_layer_id = search_resource(TOP_LAYER_KEYNAME)
    if (not point_layer_id):
        point_layer_payload = POINT_LAYER_PAYLOAD
        point_layer_payload["resource"]["parent"] = {"id": int(parent_id)}

        point_layer_id = create_resource(point_layer_payload)
        print("Point layer:", point_layer_id)

        point_geom = "POINT (0.0 0.0)"

        create_feature(point_layer_id, point_geom)
        print("Добавлена точка")
    return point_layer_id

def get_polygon_layer_id(parent_id:str) -> str:
    polygon_layer_id = search_resource(BOTTOM_LAYER_KEYNAME)
    if (not polygon_layer_id):
        polygon_layer_payload = POLYGON_LAYER_PAYLOAD
        polygon_layer_payload["resource"]["parent"] = {"id": int(parent_id)}

        polygon_layer_id = create_resource(polygon_layer_payload)
        print("Polygon layer:", polygon_layer_id)
        
        polygon_geom = "POLYGON ((10.0 30.0, 40.0 40.0, 40.0 0.0, 20.0 10.0, 10.0 30.0))"
        create_feature(polygon_layer_id, polygon_geom)
        print("Добавлен полигон")
    return polygon_layer_id


if __name__ == '__main__':
    group_id = get_resource_id()
    point_layer_id = get_point_layer_id(group_id)

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

                time.sleep(5)
