import requests
import json
import os
import schedule
import time
import pandas as pd


class ErrorConnection(Exception):
    pass

class LinkParametersHolder:
    DATA_FILE_NAME = 'data.json'

    def __init__(self, config_path='config.json'):
        try:
            with open(config_path, 'r') as config_file:
                config = json.load(config_file)

                self.ngw_host = config['ngw']['host']
                self.ngw_login = config['ngw']['login']
                self.ngw_password = config['ngw']['password']
                self.top_layer_id = config['top_layer']['id']
                self.bottom_layer_id = config['bottom_layer']['id']
                self.tmp_files_path = config['script_parametrs']['tmp_files_path']
                self.update_period_sec = config['script_parametrs']['update_period_sec']

            if __debug__:
                print(  f"hostname: {self.ngw_host}\n"
                        f"login: {self.ngw_login}\n"
                        f"password: {self.ngw_password}\n"
                        f"top layer id: {self.top_layer_id }\n"
                        f"bottom layer id: {self.bottom_layer_id}\n" 
                        f"tmp files path: {self.tmp_files_path}\n"
                        f"update period in secs: {self.update_period_sec}")
        except FileNotFoundError:
            raise ErrorConnection(f"Error: File '{config_path}' not found.")
        except json.JSONDecodeError:
            raise ErrorConnection(f"Error: File '{config_path}' contains invalid JSON.")
        except Exception as e:
            raise ErrorConnection(f"Error when opening the file '{config_path}': {e}")

    def run_script(self):
        ans = self.__get_layers_gpkg()
        if ans['status'] == 'ok':
            ans = self.__save_file_with_cur_versions()
            if ans['status'] == 'ok':
                
                top_layer_info = self.__get_latest_version_and_epoch(self.top_layer_id)
                top_layer_latest_version, top_layer_latest_epoch = top_layer_info['version'], top_layer_info['epoch'] 
                top_layer_difference = self.__get_difference_between_versions(self.top_layer_id, 0, top_layer_latest_version, top_layer_latest_epoch)
                
                bottom_layer_info = self.__get_latest_version_and_epoch(self.bottom_layer_id)
                bottom_layer_latest_version, bottom_layer_latest_epoch = bottom_layer_info['version'], bottom_layer_info['epoch'] 
                bottom_layer_difference = self.__get_difference_between_versions(self.bottom_layer_id, 0, bottom_layer_latest_version, bottom_layer_latest_epoch)

                print('getted json: ' , top_layer_difference, bottom_layer_difference)

                schedule.every(self.update_period_sec).seconds.do(lambda: self.__check_update())

                while True:
                    schedule.run_pending()
                    time.sleep(1)

    def __get_layers_gpkg(self):
        """
        This function downloads both layer in gpkg format from your webgis following the settings from config file


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
            layers_path = os.path.join(self.tmp_files_path, 'layers')
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
                message = f'Error: No rights to write GPKG files to the directory {layers_path}.'
            except FileNotFoundError:
                message = f'Error: Directory to save GPKG files - {layers_path} not exists.'
            except IOError as e:
                message = f'Input/output error when writing GPKG files: {e}'
            except Exception as e:
                message = f'Unexpected error when saving files: {e}'
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

        top_layer_info.close()
        bottom_layer_info.close()
        if __debug__:
            print(message)
        return {'status':'error', 'message':message}

    def __save_file_with_cur_versions(self):
        """
        This function saves json file with latest versions and epoch of selected layers in local directiory


        Returns
        -------
        dict
            status key contains error or ok, if error then message key contains explanations, if ok then it contains nothing else
        """
        try:
            top_layer_version_info = self.__get_latest_version_and_epoch(self.top_layer_id)
            bottom_layer_version_info = self.__get_latest_version_and_epoch(self.bottom_layer_id)

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
                file_name_and_path = os.path.join(self.tmp_files_path, "data.json")
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
            message = f"Error: Directory {self.tmp_files_path} not exists."
        except PermissionError:
            message = f"Error: No rights to write to the directory {self.tmp_files_path}."
        except (TypeError, ValueError) as e:
            message = f"Error when serializing data in JSON: {e}"
        except Exception as e:
            message = f"Unexpected error: {e}"

        if __debug__:
            print(message)
        return {'status':'error', 'message':message}

    def __check_update(self):
        """
        This function gets and writes in the console info about difference between the last saved and uploaded version to the cloud


        Returns
        -------
        dict
            status key contains error or ok, if error then message key contains explanations, if ok then it contains nothing else
        """
        # with open('data.json', 'r') as data_file:
        #     data = json.load(data_file)
        latest_version_top_layer = self.__get_latest_version_and_epoch(self.top_layer_id)
        latest_version_bottom_layer = self.__get_latest_version_and_epoch(self.bottom_layer_id)

        if latest_version_top_layer['status'] == 'ok' and latest_version_bottom_layer['status'] == 'ok':
            top_layer_info = self.__get_last_saved_version_and_epoch_by_id(self.top_layer_id)
            bottom_layer_info = self.__get_last_saved_version_and_epoch_by_id(self.bottom_layer_id)

            if top_layer_info['status'] == 'ok' and bottom_layer_info['status'] == 'ok':
                last_saved_version_top_layer, last_saved_epoch_top_layer = top_layer_info['version'], top_layer_info['epoch']
                last_saved_version_bottom_layer, last_saved_epoch_bottom_layer = bottom_layer_info['version'], bottom_layer_info['epoch']
                
                if (last_saved_version_top_layer < latest_version_top_layer['version'] and last_saved_version_bottom_layer < latest_version_bottom_layer['version']):
                    self.__get_difference_between_versions(self.top_layer_id, last_saved_version_top_layer, latest_version_top_layer['version'], last_saved_epoch_top_layer)
                    self.__get_difference_between_versions(self.bottom_layer_id, last_saved_version_bottom_layer, latest_version_bottom_layer['version'], last_saved_epoch_bottom_layer)
                    self.__save_file_with_cur_versions()
                
                elif last_saved_version_top_layer < latest_version_top_layer['version']:
                    self.__get_difference_between_versions(self.top_layer_id, last_saved_version_top_layer, latest_version_top_layer['version'], last_saved_epoch_top_layer)
                    self.__save_file_with_cur_versions()
                
                elif last_saved_version_bottom_layer < latest_version_bottom_layer['version']:
                    self.__get_difference_between_versions(self.bottom_layer_id, last_saved_version_bottom_layer, latest_version_bottom_layer['version'], last_saved_epoch_bottom_layer)
                    self.__save_file_with_cur_versions()

                else:
                    if __debug__:
                        print('From last upd nothing was changed')
                    return {'status':'ok'}

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

    def __get_latest_version_and_epoch(self, layer_id):
        """
        This function returns the last version and epoch of the layer


        Parametrs
        ---------
        layer_id : int
            unique ID of layer resource

        Returns
        -------
        dict
            status key contains error or ok, if error then message key contains explanations, if ok then version key contains the version and epoch key contains the epoch of current layer
        """
        req = f'{self.ngw_host}/api/resource/{layer_id}'
        layer_info = requests.get(req, auth = (self.ngw_login, self.ngw_password))

        if layer_info.status_code == 200:
            versioning_info = layer_info.json()['feature_layer']['versioning']

            versioning_status = versioning_info['enabled']
            if not versioning_status:
                message = f'Versioning for layer with id {layer_id} is turned off'
                if __debug__:
                    print(message)
                return {'status':'error', 'message':message}
            
            return {'status':'ok', 'version': versioning_info['latest'], 'epoch': versioning_info['epoch']}
        else:
            message = f'Request error when getting version and epoch for the layer with id {layer_id} from the server: {layer_info.status_code}'
            if __debug__:
                print(message)
            return {'status':'error', 'message':message}
    
    def __get_difference_between_versions(self, layer_id, previous_version, latest_version, epoch):
        """
        This function gets and writes in the console info about difference between the last saved and uploaded version to the cloud


        Parametrs
        ---------
        layer_id : int
            unique ID of layer resource

        previous_version : int
            the last local stored version of the layer

        latest_version : int
            the last uploaded version of the layer to the cloud

        epoch : int
            current epoch of the layer
        """
        req = f'{self.ngw_host}/api/resource/{layer_id}/feature/changes/check?epoch={epoch}&initial={previous_version}&target={latest_version}'
        difference_versions_info = requests.get(req, auth = (self.ngw_login, self.ngw_password))
        
        if difference_versions_info.status_code == 200:
            fetch = difference_versions_info.json()['fetch']
            if __debug__:
                print(f'Request link for layer with id {layer_id} between versions {previous_version} and {latest_version}: {req}')
                print(f'Link for more information: {fetch}')
            result = requests.get(fetch, auth = (self.ngw_login, self.ngw_password))
            json_result = result.json()

            time_for_versions = self.__get_versions_information(layer_id, previous_version, latest_version)
            for item in json_result:
                if 'vid' in item:
                    for time in time_for_versions:
                        if 'id' in time:
                            if time['id'] == item['vid']:
                                item['time'] = time['tstamp']
                                print("NEW VIEW", item)
            print("UPD JSON:", json_result)
                            
            return {'status':'ok', 'dif_list':result.json()}
        else:
            message = f"Error during get the list of features for the layer {layer_id} between versions {previous_version} and {latest_version} for epoch {epoch}"
            if __debug__:
                print(message)
            return {'status':'error', 'message':message}

    def __get_versions_information(self, layer_id, last_version, latest_version): # DO THERE!!!!!!!!
        """
        This function returns the list of dicts, which contain information about versions in the specified range


        Parametrs
        ---------
        layer_id : int
            unique ID of layer resource

        previous_version : int
            the last local stored version of the layer

        latest_version : int
            the last uploaded version of the layer to the cloud
        """
        answer = []
        for version in range(last_version, latest_version):
            req = f'{self.ngw_host}/api/resource/{layer_id}/feature/version/{version}'
            print('Request link to get version info: ', req)
            current_version_info = requests.get(req, auth = (self.ngw_login, self.ngw_password))
            if current_version_info.status_code == 200:
                answer.append(current_version_info.json())
            else:
                if __debug__:
                    print(f"Error while getting feature of version {version} for the layer with id {layer_id}")
        return {'status':'ok', 'info':answer}

    def __get_last_saved_version_and_epoch_by_id(self, layer_id):
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
            data_file_name_and_path = os.path.join(self.tmp_files_path, LinkParametersHolder.DATA_FILE_NAME)
            with open(data_file_name_and_path, 'r') as data_file:
                data = json.load(data_file)

            for layer in data.values():
                if layer['id'] == layer_id:
                    return {'status':'ok', 'version':layer['version'], 'epoch':layer['epoch']}

            message = f'Error when finding local info for the layer with id: {layer_id}'
            if __debug__:
                print(message)
            return {'status':'error', 'message':message}
        except FileNotFoundError:
            message = f"Error: File '{data_file_name_and_path}' not found."
        except json.JSONDecodeError:
            message = f"Error: File '{data_file_name_and_path}' contains invalid JSON."
        except Exception as e:
            message = f"Error when opening the file '{data_file_name_and_path}': {e}"

        if __debug__:
            print(message)
        return {'status':'error', 'message':message}

def main(config_path):
    new_lph = LinkParametersHolder(config_path)
    new_lph.run_script()

if __name__ == '__main__':
    try:
        import sys
        print(sys.argv)
        main('config.json')
    except ErrorConnection as e:
        print(e)




def get_layer_info(ngw_host, ngw_login, ngw_password, layer_id):
    req = ngw_host + '/api/resource/' + str(layer_id)
    layer_info = requests.get(req, auth = (ngw_login, ngw_password))
    print(layer_info.json())