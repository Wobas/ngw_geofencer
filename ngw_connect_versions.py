import requests
import json
import os
import schedule
import time
import pandas as pd


class ErrorConnection(Exception):
    pass

class LinkParametersHolder:
    PATH_TO_SAVE_TMP_FILES = os.path.dirname(os.path.abspath(__file__))
    CONFIG_NAME = 'config.json'
    DATA_FILE_NAME = 'data.json'

    def __init__(self):
        try:
            with open(LinkParametersHolder.CONFIG_NAME, 'r') as config_file:
                config = json.load(config_file)

                self.ngw_host = config['ngw']['host']
                self.ngw_login = config['ngw']['login']
                self.ngw_password = config['ngw']['password']
                self.top_layer_id = config['top_layer']['id']
                self.bottom_layer_id = config['bottom_layer']['id']

            if __debug__:
                print(f"hostname: {config['ngw']['host']} \nlogin: {config['ngw']['login']} \npassword: {config['ngw']['password']} \ntop layer id: {config['top_layer']['id']} \nbottom layer id: {config['bottom_layer']['id']}")
        except FileNotFoundError:
            raise ErrorConnection(f"Error: File '{LinkParametersHolder.CONFIG_NAME}' not found.")
        except json.JSONDecodeError:
            raise ErrorConnection(f"Error: File '{LinkParametersHolder.CONFIG_NAME}' contains invalid JSON.")
        except Exception as e:
            raise ErrorConnection(f"Error when opening the file '{LinkParametersHolder.CONFIG_NAME}': {e}")

    def get_layers_gpkg(self):
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

        Returns
        -------
        dict
            status key contains error or ok, if error then message key contains explanations, if ok then it contains nothing else
        """
        req_top_layer = f'{self.ngw_host}/api/resource/{self.top_layer_id}/export?context=IFeatureLayer&format=GPKG&zipped=false'
        req_bottom_layer = f'{self.ngw_host}/api/resource/{self.bottom_layer_id}/export?context=IFeatureLayer&format=GPKG&zipped=false'
        # переделать через with
        top_layer_info = requests.get(req_top_layer, stream = True, auth = (self.ngw_login, self.ngw_password))
        bottom_layer_info = requests.get(req_bottom_layer, stream = True, auth = (self.ngw_login, self.ngw_password))

        if top_layer_info.status_code == 200 and bottom_layer_info.status_code == 200:
            layers_path = os.path.join(self.PATH_TO_SAVE_TMP_FILES, 'layers')
            file_name_and_path_top_layer = os.path.join(layers_path, f'layer_{self.top_layer_id}.gpkg')
            file_name_and_path_bottom_layer = os.path.join(layers_path, f'layer_{self.bottom_layer_id}.gpkg')
            
            try:
                with open(file_name_and_path_top_layer, 'wb') as file:
                    for chunk in top_layer_info.iter_content(chunk_size=8192): 
                        file.write(chunk)

                with open(file_name_and_path_bottom_layer, 'wb') as file:
                    for chunk in bottom_layer_info.iter_content(chunk_size=8192): 
                        file.write(chunk)

                if __debug__:
                    print(f'GPKG files was successfully saved in {file_name_and_path_top_layer} and {file_name_and_path_bottom_layer}')
                return {'status':'ok'}
            except PermissionError:
                message = f'Error: No rights to write to the directory {layers_path}.'
            except FileNotFoundError:
                message = f'Error: Directory {layers_path} not exists.'
            except IOError as e:
                message = f'Input/output error when writing a GPKG file: {e}'
            except Exception as e:
                message = f'Unexpected error when saving a file: {e}'
            finally:
                # close the connection anyway (due to stream = True)
                top_layer_info.close()
                bottom_layer_info.close()
            if __debug__:
                print(message)
            return {'status':'error', 'message':message}
        
        elif top_layer_info.status_code != 200 and bottom_layer_info.status_code != 200:
            message = f'Request errors: {top_layer_info.status_code} AND {bottom_layer_info.status_code}'
        
        elif top_layer_info.status_code != 200:
            message = f'Request error: {top_layer_info.status_code}'
        
        elif bottom_layer_info.status_code != 200:
            message = f'Request error: {bottom_layer_info.status_code}'

        if __debug__:
            print(message)
        return {'status':'error', 'message':message}

    def save_file_with_cur_versions(self):
        """
        This function saves json file with latest versions and epoch of selected layers in specified directiory


        Parametrs
        ---------
        ngw_host : str
            address of ngw instance, e.g. demo.nextgis.com

        ngw_login : str
            your longin for auth

        ngw_password : str
            your password for auth

        top_layer_id : int
            unique ID of top layer resource

        bottom_layer_id : int
            unique ID of bottom layer resource
            
        path_to_save : str
            local address to store files

        Returns
        -------
        dict
            status key contains error or ok, if error then message key contains explanations, if ok then it contains nothing else
        """
        try:
            top_layer_version_info = get_latest_version_and_epoch(self.ngw_host, self.ngw_login, self.ngw_password, self.top_layer_id)
            bottom_layer_version_info = get_latest_version_and_epoch(self.ngw_host, self.ngw_login, self.ngw_password, self.bottom_layer_id)

            if top_layer_version_info['status'] == 'ok' and bottom_layer_version_info['status'] == 'ok':
                if not all(key in top_layer_version_info for key in ['version', 'epoch']):
                    raise KeyError("Missing 'version' or 'epoch' in top_layer_version_info")
                if not all(key in bottom_layer_version_info for key in ['version', 'epoch']):
                    raise KeyError("Missing 'version' or 'epoch' in bottom_layer_version_info")

                data = {
                    "top_layer": {
                        "id":self.top_layer_id,
                        "version":top_layer_version_info['version'],
                        "epoch":top_layer_version_info['epoch']
                    },
                    "bottom_layer": {
                        "id":self.bottom_layer_id,
                        "version":bottom_layer_version_info['version'],
                        "epoch":bottom_layer_version_info['epoch']
                    }
                }
                file_name_and_path = os.path.join(self.PATH_TO_SAVE_TMP_FILES, "data.json")
                with open(file_name_and_path, "w", encoding="utf-8") as json_file:
                    json.dump(data, json_file, ensure_ascii=False, indent=4)

                if __debug__:
                    print(f'Data file was successfully saved in {file_name_and_path}')
                return {'status':'ok'}
            else:
                message = f'Error when reading data file. Top layer status: {top_layer_version_info['message']}; Bottom layer status: {bottom_layer_version_info}'
                if __debug__:
                    print()
                return {'status':'error', 'message':message}
        except KeyError as e:
            message = f"Error: {e}"
        except FileNotFoundError:
            message = f"Error: Directory {self.PATH_TO_SAVE_TMP_FILES} not exists."
        except PermissionError:
            message = f"Error: No rights to write to the directory {self.PATH_TO_SAVE_TMP_FILES}."
        except (TypeError, ValueError) as e:
            message = f"Error when serializing data in JSON: {e}"
        except Exception as e:
            message = f"Unexpected error: {e}"

        if __debug__:
            print(message)
        return {'status':'error', 'message':message}

    def check_update(self):
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
        latest_version_top_layer = get_latest_version_and_epoch(self.ngw_host, self.ngw_login, self.ngw_password, self.top_layer_id)
        latest_version_bottom_layer = get_latest_version_and_epoch(self.ngw_host, self.ngw_login, self.ngw_password, self.bottom_layer_id)

        if latest_version_top_layer['status'] == 'ok' and latest_version_bottom_layer['status'] == 'ok':
            top_layer_info = get_last_saved_version_and_epoch_by_id(self.top_layer_id)
            bottom_layer_info = get_last_saved_version_and_epoch_by_id(self.bottom_layer_id)

            if top_layer_info['status'] == 'ok' and bottom_layer_info['status'] == 'ok':
                last_saved_version_top_layer, last_saved_epoch_top_layer = top_layer_info['version'], top_layer_info['epoch']
                last_saved_version_bottom_layer, last_saved_epoch_bottom_layer = bottom_layer_info['version'], bottom_layer_info['epoch']
                
                if (last_saved_version_top_layer < latest_version_top_layer['version'] and last_saved_version_bottom_layer < latest_version_bottom_layer['version']):
                    get_difference_between_versions(self.ngw_host, self.ngw_login, self.ngw_password, self.top_layer_id, last_saved_version_top_layer, latest_version_top_layer['version'], last_saved_epoch_top_layer)
                    get_difference_between_versions(self.ngw_host, self.ngw_login, self.ngw_password, self.bottom_layer_id, last_saved_version_bottom_layer, latest_version_bottom_layer['version'], last_saved_epoch_bottom_layer)
                    self.save_file_with_cur_versions()
                
                elif last_saved_version_top_layer < latest_version_top_layer['version']:
                    get_difference_between_versions(self.ngw_host, self.ngw_login, self.ngw_password, self.top_layer_id, last_saved_version_top_layer, latest_version_top_layer['version'], last_saved_epoch_top_layer)
                    self.save_file_with_cur_versions()
                
                elif last_saved_version_bottom_layer < latest_version_bottom_layer['version']:
                    get_difference_between_versions(self.ngw_host, self.ngw_login, self.ngw_password, self.bottom_layer_id, last_saved_version_bottom_layer, latest_version_bottom_layer['version'], last_saved_epoch_bottom_layer)
                    self.save_file_with_cur_versions()

                if __debug__:
                    print('From last upd nothing was changed')
                return {'status':'ok'}
            
            else:
                message = f'Error when getting last saved version of layers. For top layer: {top_layer_info['message']}; For bottom layer: {bottom_layer_info['message']}'
                if __debug__:
                    print(message)
                return {'status':'error', 'message':message}
        else:
            message = f'Error when getting version of layers. For top layer: {latest_version_top_layer['message']}; For bottom layer: {latest_version_bottom_layer['message']}'
            if __debug__:
                print(message)
            return {'status':'error', 'message':message}

def get_latest_version_and_epoch(ngw_host, ngw_login, ngw_password, layer_id):
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
        status key contains error or ok, if error then message key contains explanations, if ok then version key contains the version and epoch key contains the epoch of current layer
    """
    req = f'{ngw_host}/api/resource/{layer_id}'
    layer_info = requests.get(req, auth = (ngw_login, ngw_password))

    if layer_info.status_code == 200:
        versioning_info = layer_info.json()['feature_layer']['versioning']

        versioning_status = versioning_info['enabled']
        if not versioning_status:
            return {'status':'error', 'message':'Versioning is turned off'}
        
        return{'status':'ok', 'version': versioning_info['latest'], 'epoch': versioning_info['epoch']}
    else:
        message = f'Request error: {layer_info.status_code}'
        if __debug__:
            print(message)
        return {'status':'error', 'message':message}
    
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
    req = f'{ngw_host}/api/resource/{layer_id}/feature/changes/check?epoch={epoch}&initial={previous_version}&target={latest_version}'
    difference_versions_info = requests.get(req, auth = (ngw_login, ngw_password))
    fetch = difference_versions_info.json()['fetch']
    if __debug__:
        print(f'Request link for layer with id {layer_id} between versions {previous_version} and {latest_version}: {req}')
        print(f'Link for more information: {fetch}')
    result = requests.get(fetch, auth = (ngw_login, ngw_password))

    json_result = result.json()
    print("JSON WITH UPDS: ", json_result)

    time_for_versions = get_versions_information(ngw_host, ngw_login, ngw_password, layer_id, previous_version, latest_version)
    # print("JSON WITH TIME:", time_for_versions)

    for item in json_result:
        if 'vid' in item:
            for time in time_for_versions:
                if 'id' in time:
                    if time['id'] == item['vid']:
                        item['time'] = time['tstamp']
                        print("NEW VIEW", item)
    print("UPD JSON:", json_result)
    # for item in result.json():
    #     # print("current item: ", item)
    #     if 'vid' in item:
    #         merged_data[item['vid']] = item
    
    # for item in time_for_versions:
    #     if 'id' in item:
    #         print("current item: ", item)
    #         for target_item in merged_data:
    #             if item['id'] == target_item['vid']:
    #                 merged_data[item['id']].update(item)
                    
    return(result.json())

def get_versions_information(ngw_host, ngw_login, ngw_password, layer_id, last_version, latest_version):
    answer = []
    for version in range(last_version, latest_version):
        req = f'{ngw_host}/api/resource/{layer_id}/feature/version/{version}'
        print('Request link to get version info: ', req)
        current_version_info = requests.get(req, auth = (ngw_login, ngw_password))
        answer.append(current_version_info.json())
    return(answer)

def get_last_saved_version_and_epoch_by_id(layer_id):
    """
    This function returns last local saved version and epoch of the layer


    Parametrs
    ---------
    layer_id : int
        unique ID of layer resource

    Returns
    -------
    dict
        status key contains error or ok, if error then message key contains explanations, if ok then version key contains the last saved version and epoch key contains the last saved epoch of current layer
    """
    try:
        with open(LinkParametersHolder.DATA_FILE_NAME, 'r') as data_file:
            data = json.load(data_file)

        for layer in data.values():
            if layer['id'] == layer_id:
                return {'status':'ok', 'version':layer['version'], 'epoch':layer['epoch']}

        message = f'Error when finding local info for the layer with id: {layer_id}'
        if __debug__:
            print(message)
        return {'status':'error', 'message':message}
    except FileNotFoundError:
        message = f"Error: File '{LinkParametersHolder.DATA_FILE_NAME}' not found."
    except json.JSONDecodeError:
        message = f"Error: File '{LinkParametersHolder.DATA_FILE_NAME}' contains invalid JSON."
    except Exception as e:
        message = f"Error when opening the file '{LinkParametersHolder.DATA_FILE_NAME}': {e}"

    if __debug__:
        print(message)
    return {'status':'error', 'message':message}

def main():
    # create new object to store current link parametrers and fill it by reading config
    new_lph = LinkParametersHolder()

    # get initial state of layers
    get_layers_status = new_lph.get_layers_gpkg()
    if (get_layers_status['status'] == 'ok'):
        top_layer_info = get_latest_version_and_epoch(new_lph.ngw_host, new_lph.ngw_login, new_lph.ngw_password, new_lph.top_layer_id)
        top_layer_latest_version, top_layer_latest_epoch = top_layer_info['version'], top_layer_info['epoch'] 
        top_layer_difference = get_difference_between_versions(new_lph.ngw_host, new_lph.ngw_login, new_lph.ngw_password, new_lph.top_layer_id, 0, top_layer_latest_version, top_layer_latest_epoch)
        
        bottom_layer_info = get_latest_version_and_epoch(new_lph.ngw_host, new_lph.ngw_login, new_lph.ngw_password, new_lph.bottom_layer_id)
        bottom_layer_latest_version, bottom_layer_latest_epoch = bottom_layer_info['version'], bottom_layer_info['epoch'] 
        bottom_layer_difference = get_difference_between_versions(new_lph.ngw_host, new_lph.ngw_login, new_lph.ngw_password, new_lph.bottom_layer_id, 0, bottom_layer_latest_version, bottom_layer_latest_epoch)
        
        print('getted json: ' , top_layer_difference, bottom_layer_difference)
        # create file with current versions of layers
        new_lph.save_file_with_cur_versions()

        # setting a data update schedule
        schedule.every().minute.do(lambda: new_lph.check_update())

        while True:
            schedule.run_pending()
            time.sleep(1)

    elif (get_layers_status['status'] == 'error'):
        print(get_layers_status['message'])

if __name__ == '__main__':
    try:
        main()
    except ErrorConnection as e:
        print(e)




def get_layer_info(ngw_host, ngw_login, ngw_password, layer_id):
    req = ngw_host + '/api/resource/' + str(layer_id)
    layer_info = requests.get(req, auth = (ngw_login, ngw_password))
    print(layer_info.json())