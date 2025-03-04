import requests
import json
import os
import schedule
import time
import pandas as pd

ngw_host = ""
ngw_login = ""
ngw_password = ""
top_layer_id = 0
bottom_layer_id = 0

path_to_save_tmp_files = os.path.dirname(os.path.abspath(__file__))

def read_config():
    """
    This function sets the main parameters for the script
    """
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    global ngw_host, ngw_login, ngw_password, top_layer_id, bottom_layer_id
    ngw_host = config['ngw']['host']
    ngw_login = config['ngw']['login']
    ngw_password = config['ngw']['password']

    top_layer_id = config['top_layer']['id']

    bottom_layer_id = config['bottom_layer']['id']

def get_layer_info(ngw_host, ngw_login, ngw_password, layer_id):
    req = ngw_host + '/api/resource/' + str(layer_id)
    layer_info = requests.get(req, auth = (ngw_login, ngw_password))
    print(layer_info.json())

def get_layer_gpkg(ngw_host, ngw_login, ngw_password, layer_id, save_directory):
    """
    This function downloads the layer in gpkg format from your webgis


    Parametrs
    ---------
    ngw_host : str
        address of ngw instance, e.g. demo.nextgis.com

    ngw_login : str
        your longin for auth

    ngw_password : str
        your password for auth

    layer_id : int
        unique ID of layer resource

    save_directory : str
        local directory to save gpkg file
    """
    req = ngw_host + '/api/resource/' + str(layer_id) + '/export?context=IFeatureLayer&format=GPKG&zipped=false'
    layer_info = requests.get(req, auth = (ngw_login, ngw_password))

    if layer_info.status_code == 200:
        save_path = os.path.join(save_directory, f'layer_{layer_id}.gpkg')
        
        with open(save_path, 'wb') as file:
            file.write(layer_info.content)
        print(f'Файл успешно сохранен в {save_path}')
    else:
        print(f'Ошибка при запросе: {layer_info.status_code}')

def get_latest_version(ngw_host, ngw_login, ngw_password, layer_id):
    """
    This function returns the last version and epoch of the layer


    Parametrs
    ---------
    ngw_host : str
        address of ngw instance, e.g. demo.nextgis.com

    ngw_login : str
        your longin for auth

    ngw_password : str
        your password for auth

    layer_id : int
        unique ID of layer resource

    Returns
    -------
    dict
        status key contains error or ok, if error then message key contains explanations, if ok then data key contains version and epoch of current layer
    """
    req = ngw_host + '/api/resource/' + str(layer_id)
    layer_info = requests.get(req, auth = (ngw_login, ngw_password))
    versioning_info = layer_info.json()['feature_layer']['versioning']

    versioning_status = versioning_info['enabled']
    if not versioning_status:
        return {'status':'error', 'message':'Versioning is turned off'}
    
    return{'status':'ok', 'version': versioning_info['latest'], 'epoch': versioning_info['epoch']}

def get_versions_changes(ngw_host, ngw_login, ngw_password, layer_id, latest_version):
    for version in range(latest_version, 0, -1):
        req = ngw_host + '/api/resource/' + str(layer_id) + '/feature/version/' + str(version)
        print(req)
        current_version_info = requests.get(req, auth = (ngw_login, ngw_password))
        print(current_version_info.json())

def save_file_with_cur_versions(path_to_save):
    """
    This function saves json file with latest versions and epoch of selected layers in specified directiory


    Parametrs
    ---------
    path_to_save : str
        local address to store files
    """
    data = {
        "top_layer": {
            "id":top_layer_id,
            "version":get_latest_version(ngw_host, ngw_login, ngw_password, top_layer_id)['version'],
            "epoch":get_latest_version(ngw_host, ngw_login, ngw_password, top_layer_id)['epoch']
        },
        "bottom_layer": {
            "id":bottom_layer_id,
            "version":get_latest_version(ngw_host, ngw_login, ngw_password, bottom_layer_id)['version'],
            "epoch":get_latest_version(ngw_host, ngw_login, ngw_password, bottom_layer_id)['epoch']
        }
    }
    file_path = os.path.join(path_to_save, "data.json")
    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)
    
def get_difference_between_versions(ngw_host, ngw_login, ngw_password, layer_id, previous_version, latest_version, epoch):
    """
    This function gets and writes in the console info about difference between the last saved and uploaded version to the cloud


    Parametrs
    ---------
    ngw_host : str
        address of ngw instance, e.g. demo.nextgis.com

    ngw_login : str
        your longin for auth

    ngw_password : str
        your password for auth

    layer_id : int
        unique ID of layer resource

    previous_version : int
        the last local stored version of the layer

    latest_version : int
        the last uploaded version of the layer to the cloud

    epoch : int
        current epoch of the layer
    """
    req = ngw_host + '/api/resource/' + str(layer_id) + '/feature/changes/check?epoch=' + str(epoch) + '&initial=' + str(previous_version) + '&target=' + str(latest_version)
    difference_versions_info = requests.get(req, auth = (ngw_login, ngw_password))
    print(req)
    fetch = difference_versions_info.json()['fetch']
    print(fetch)
    result = requests.get(fetch, auth = (ngw_login, ngw_password))
    print(result.json())

def check_update(ngw_host, ngw_login, ngw_password, layer_id):
    """
    This function gets and writes in the console info about difference between the last saved and uploaded version to the cloud


    Parametrs
    ---------
    ngw_host : str
        address of ngw instance, e.g. demo.nextgis.com

    ngw_login : str
        your longin for auth

    ngw_password : str
        your password for auth

    layer_id : int
        unique ID of layer resource

    epoch : int
        current epoch of the layer
    """
    # with open('data.json', 'r') as data_file:
    #     data = json.load(data_file)
    latest_version = get_latest_version(ngw_host, ngw_login, ngw_password, layer_id)['version']
    last_saved_version, epoch = get_version_and_epoch_by_id(layer_id)
    if (last_saved_version < latest_version):
        get_difference_between_versions(ngw_host, ngw_login, ngw_password, layer_id, last_saved_version, latest_version, epoch)
        save_file_with_cur_versions(path_to_save_tmp_files)
    else:
        return

def get_version_and_epoch_by_id(layer_id):
    with open('data.json', 'r') as data_file:
        data = json.load(data_file)
    for layer in data.values():
        if layer['id'] == layer_id:
            return layer['version'], layer['epoch']
    return None, None

# read config to get info to run the script
read_config()

# get initial state of layers
get_layer_gpkg(ngw_host, ngw_login, ngw_password, top_layer_id, path_to_save_tmp_files+"\layers")
get_layer_gpkg(ngw_host, ngw_login, ngw_password, bottom_layer_id, path_to_save_tmp_files+"\layers")

# create file with current versions of layers
save_file_with_cur_versions(path_to_save_tmp_files)

# setting a data update schedule
schedule.every().minute.do(lambda: check_update(ngw_host, ngw_login, ngw_password, top_layer_id))

while True:
    schedule.run_pending()
    time.sleep(1)