import requests
import json
import os
import schedule
import pandas as pd

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

ngw_host = config['ngw']['host']
ngw_login = config['ngw']['login']
ngw_password = config['ngw']['password']

top_layer_id = config['top_layer']['id']

bottom_layer_id = config['bottom_layer']['id']

def get_layer_info(ngw_host, ngw_login, ngw_password, layer_id):
    req = ngw_host + '/api/resource/' + str(layer_id)
    layer_info = requests.get(req, auth = (ngw_login, ngw_password))
    print(layer_info.json())

# get_layer_info(ngw_host, ngw_login, ngw_password, top_layer_id)

def get_layer_gpkg(ngw_host, ngw_login, ngw_password, layer_id, save_directory):
    req = ngw_host + '/api/resource/' + str(layer_id) + '/export?context=IFeatureLayer&format=GPKG&zipped=false'
    layer_info = requests.get(req, auth = (ngw_login, ngw_password))

    if layer_info.status_code == 200:
        # Создаем путь для сохранения файла
        save_path = os.path.join(save_directory, f'layer_{layer_id}.gpkg')
        
        # Сохраняем содержимое ответа в файл
        with open(save_path, 'wb') as file:
            file.write(layer_info.content)
        print(f'Файл успешно сохранен в {save_path}')
    else:
        print(f'Ошибка при запросе: {layer_info.status_code}')

# schedule.every(10).minutes.do(get_layer_gpkg(ngw_host, ngw_login, ngw_password, top_layer_id, os.path.dirname(os.path.abspath(__file__))+"/layers"))

def get_latest_version(ngw_host, ngw_login, ngw_password, layer_id):
    req = ngw_host + '/api/resource/' + str(layer_id)
    layer_info = requests.get(req, auth = (ngw_login, ngw_password))
    versioning_info = layer_info.json()['feature_layer']['versioning']

    versioning_status = versioning_info['enabled']
    if not versioning_status:
        return {'status':'error', 'message':'Versioning is turned off'}
    
    return{'status':'ok', 'version': versioning_info['latest']}

def get_versions_changes(ngw_host, ngw_login, ngw_password, layer_id, latest_version):
    for version in range(latest_version, 0, -1):
        req = ngw_host + '/api/resource/' + str(layer_id) + '/feature/version/' + str(version)
        print(req)
        current_version_info = requests.get(req, auth = (ngw_login, ngw_password))
        print(current_version_info.json())

get_latest_version(ngw_host, ngw_login, ngw_password, top_layer_id)
# print(ngw_host, ngw_login, ngw_password, top_layer_id, bottom_layer_id)