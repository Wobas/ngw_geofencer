import requests


def get_ngw_layer_features (ngw_host : str, resource_id : int, user_name : str, user_password : str) -> dict:
    """
    returns features of ngw vector layer based on HTTP API


    Parametrs
    ---------
    ngw_host : str
        address of ngw instance, e.g. demo.nextgis.com

    resource_id : int
        unique ID of layer resource

    user_name : str
        your longin for auth

    user_password : str
        your password for auth

    Returns
    -------
    dict
        status key contains error or ok, if error then message key contains explanations, if ok then data key contains features 
    """
    # format: resource, features,
    url = f"{ngw_host}/api/resource/{resource_id}/feature/"
    response = requests.get(url)
    if response.status_code != 200:
        return { 
            'status' : 'error',
            'message' : response.status_code
        }
    else:
        return {
            'status' : 'ok',
            'data' : response.json()
        }