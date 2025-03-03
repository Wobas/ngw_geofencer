import requests
import json

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

ngw_host = config['ngw']['host']
ngw_login = config['ngw']['login']
ngw_password = config['ngw']['password']

top_layer_id = config['top_layer']['id']

bottom_layer_id = config['bottom_layer']['id']

print(ngw_host, ngw_login, ngw_password, top_layer_id, bottom_layer_id)


