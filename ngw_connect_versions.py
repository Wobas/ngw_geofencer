import requests
import json
import jsonschema
from jsonschema import validate
import os
import schedule
import time
import base64
from datetime import datetime
import pandas as pd
import geopandas as gpd
import shapely
from shapely.geometry import Point, Polygon
from shapely.wkt import loads
from osgeo import gdal, ogr, osr
ogr.UseExceptions()


class ErrorConnection(Exception):
    pass

class NGWGeofencer:
    DATA_FILE_NAME = 'data.json'
    CONFIG_SCHEMA = {
        "type": "object",
        "properties": {
            "ngw": {
                "type": "object",
                "properties": {
                    "host": {"type": "string", "format": "uri"},
                    "login": {"type": "string"},
                    "password": {"type": "string"},
                },
                "required": ["host", "login", "password"],
            },
            "top_layer": {
                "type": "object",
                "properties": {
                    "id": {"type": "number"},
                },
                "required": ["id"],
            },
            "bottom_layer": {
                "type": "object",
                "properties": {
                    "id": {"type": "number"},
                },
                "required": ["id"],
            },
            "script_parametrs": {
                "type": "object",
                "properties": {
                    "geofence_mode": {"type": "string"},
                    "tmp_files_path": {"type": "string"},
                    "update_period_sec": {"type": "number"},
                    "message_type": {"type": "string"},
                },
                "required": ["geofence_mode", "tmp_files_path", "update_period_sec", "message_type"],
            },
        },
        "required": ["ngw", "top_layer", "bottom_layer", "script_parametrs"],
    }

    def __init__(self, config_path='config.json'):
        try:
            with open(config_path, 'r') as config_file:
                config = json.load(config_file)
                
                validate(instance=config, schema=self.CONFIG_SCHEMA)

                self.ngw_host = config['ngw']['host']
                self.ngw_login = config['ngw']['login']
                self.ngw_password = config['ngw']['password']
                self.top_layer_id = config['top_layer']['id']
                self.bottom_layer_id = config['bottom_layer']['id']
                self.geofence_mode = config['script_parametrs']['geofence_mode']
                self.tmp_files_path = config['script_parametrs']['tmp_files_path']
                self.update_period_sec = config['script_parametrs']['update_period_sec']
                self.message_type = config['script_parametrs']['message_type']

            if __debug__:
                print(  f"hostname: {self.ngw_host}\n"
                        f"login: {self.ngw_login}\n"
                        f"password: {self.ngw_password}\n"
                        f"top layer id: {self.top_layer_id }\n"
                        f"bottom layer id: {self.bottom_layer_id}\n"
                        f"geofence mode: {self.geofence_mode}\n"
                        f"tmp files path: {self.tmp_files_path}\n"
                        f"update period in secs: {self.update_period_sec}\n"
                        f"message type: {self.message_type}\n")
        except FileNotFoundError:
            raise ErrorConnection(f"Error: File '{config_path}' not found.")
        except json.JSONDecodeError:
            raise ErrorConnection(f"Error: File '{config_path}' contains invalid JSON.")
        except jsonschema.ValidationError as e:
            raise ErrorConnection(f"Ошибка в конфигурации файла: {e.message}")
        except Exception as e:
            raise ErrorConnection(f"Error when opening the file '{config_path}': {e}")

    def send_message(self, message):
        print(message)

    def run_script(self):
        status = self.__get_layers_gpkg()
        if status['status'] == 'ok':
            status = self.__save_file_with_cur_versions()
            if status['status'] == 'ok':
                schedule.every(self.update_period_sec).seconds.do(lambda: self.__check_update())

                while True:
                    schedule.run_pending()
                    time.sleep(1)

    def __get_layers_gpkg(self):
        """
        This function downloads both layers in gpkg format from your webgis following the settings from config file


        Returns
        -------
        dict
            status key contains error or ok, if error then message key contains explanations, if ok then it contains nothing else
        """
        req_top_layer = f'{self.ngw_host}/api/resource/{self.top_layer_id}/export?context=IFeatureLayer&format=GPKG&zipped=false'
        req_bottom_layer = f'{self.ngw_host}/api/resource/{self.bottom_layer_id}/export?context=IFeatureLayer&format=GPKG&zipped=false'
        top_layer_info = requests.get(req_top_layer, stream = True, auth = (self.ngw_login, self.ngw_password))
        bottom_layer_info = requests.get(req_bottom_layer, stream = True, auth = (self.ngw_login, self.ngw_password))

        if top_layer_info.status_code == 200 and bottom_layer_info.status_code == 200:            
            layers_path = os.path.join(self.tmp_files_path, 'layers')
            if not os.path.isdir(layers_path): os.makedirs(layers_path)
            
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
                    print(f'GPKG files was successfully saved in {file_name_and_path_top_layer} and {file_name_and_path_bottom_layer}\n')
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
        else: message = f'Request errors when getting GPKG files! Top layer status: {top_layer_info.status_code}; Bottom layer status: {bottom_layer_info.status_code}'

        top_layer_info.close()
        bottom_layer_info.close()

        return self.__handle_error(message)

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
                file_name_and_path = os.path.join(self.tmp_files_path, self.DATA_FILE_NAME)
                with open(file_name_and_path, "w", encoding="utf-8") as json_file:
                    json.dump(data, json_file, ensure_ascii=False, indent=4)

                if __debug__:
                    print(f'Data file was successfully saved in {file_name_and_path}\n')
                return {'status':'ok'}
            else: message = f'Error when reading data from the server to save data file. Top layer status: {top_layer_version_info['message']}; Bottom layer status: {bottom_layer_version_info['message']}'

        except KeyError as e:
            message = f"Error when saving data file: {e}"
        except FileNotFoundError:
            message = f"Error when saving data file: Directory {self.tmp_files_path} not exists."
        except PermissionError:
            message = f"Error when saving data file: No rights to write to the directory {self.tmp_files_path}."
        except (TypeError, ValueError) as e:
            message = f"Error when serializing data in JSON: {e}"
        except Exception as e:
            message = f"Unexpected error when saving data file: {e}"

        return self.__handle_error(message)

    def __check_update(self):
        """
        This function gets and writes in the console info about difference between the last saved and uploaded version to the cloud


        Returns
        -------
        dict
            status key contains error or ok, if error then message key contains explanations, if ok then it contains nothing else
        """
        latest_version_top_layer = self.__get_latest_version_and_epoch(self.top_layer_id)
        latest_version_bottom_layer = self.__get_latest_version_and_epoch(self.bottom_layer_id)

        if latest_version_top_layer['status'] == 'ok' and latest_version_bottom_layer['status'] == 'ok':
            top_layer_info = self.__get_last_saved_version_and_epoch_by_id(self.top_layer_id)
            bottom_layer_info = self.__get_last_saved_version_and_epoch_by_id(self.bottom_layer_id)

            if top_layer_info['status'] == 'ok' and bottom_layer_info['status'] == 'ok':
                last_saved_version_top_layer, last_saved_epoch_top_layer = top_layer_info['version'], top_layer_info['epoch']
                last_saved_version_bottom_layer, last_saved_epoch_bottom_layer = bottom_layer_info['version'], bottom_layer_info['epoch']
                
                if (last_saved_version_top_layer < latest_version_top_layer['version'] and last_saved_version_bottom_layer < latest_version_bottom_layer['version']):
                    top_layer_dif_info = self.__get_difference_between_versions(self.top_layer_id, last_saved_version_top_layer, latest_version_top_layer['version'], last_saved_epoch_top_layer)
                    bottom_layer_dif_info = self.__get_difference_between_versions(self.bottom_layer_id, last_saved_version_bottom_layer, latest_version_bottom_layer['version'], last_saved_epoch_bottom_layer)
                    
                    if (top_layer_dif_info['status'] == 'ok' and bottom_layer_dif_info['status'] == 'ok'):
                        save_file_info = self.__save_file_with_cur_versions()
                        if (save_file_info['status'] == 'ok'):
                            both_layers_differences = sorted(top_layer_dif_info['dif_list']+bottom_layer_dif_info['dif_list'], key=self.__get_time)
                            self.__check_geometry(both_layers_differences)
                            return {'status':'ok'}
                        else: message = save_file_info['message']
                    else: message = f'Error when getting difference list of features. For top layer: {top_layer_dif_info['message']}; For bottom layer: {bottom_layer_dif_info['message']}'
                
                elif last_saved_version_top_layer < latest_version_top_layer['version']:
                    top_layer_dif_info = self.__get_difference_between_versions(self.top_layer_id, last_saved_version_top_layer, latest_version_top_layer['version'], last_saved_epoch_top_layer)
                    
                    if (top_layer_dif_info['status'] == 'ok'):
                        save_file_info = self.__save_file_with_cur_versions()
                        if (save_file_info['status'] == 'ok'):
                            layer_differences = sorted(top_layer_dif_info['dif_list'], key=self.__get_time)
                            self.__check_geometry(layer_differences)
                            return {'status':'ok'}
                        else: message = save_file_info['message']
                    else: message = f'Error when getting difference list of features. For top layer: {top_layer_dif_info['message']}'
                
                elif last_saved_version_bottom_layer < latest_version_bottom_layer['version']:
                    bottom_layer_dif_info = self.__get_difference_between_versions(self.bottom_layer_id, last_saved_version_bottom_layer, latest_version_bottom_layer['version'], last_saved_epoch_bottom_layer)
                    
                    if (bottom_layer_dif_info['status'] == 'ok'):
                        save_file_info = self.__save_file_with_cur_versions()
                        if (save_file_info['status'] == 'ok'):
                            layer_differences = sorted(bottom_layer_dif_info['dif_list'], key=self.__get_time)
                            self.__check_geometry(layer_differences)
                            return {'status':'ok'}
                        else: message = save_file_info['message']
                    else: message = f'Error when getting difference list of features. For bottom layer: {bottom_layer_dif_info['message']}'

                else:
                    if __debug__:
                        self.send_message(datetime.now().strftime("%H:%M:%S")+' From last upd nothing was changed')
                    return {'status':'ok'}    
            else: message = f'Error when getting last saved version of layers. For top layer: {top_layer_info['message']}; For bottom layer: {bottom_layer_info['message']}'
        else: message = f'Error when getting version of layers. For top layer: {latest_version_top_layer['message']}; For bottom layer: {latest_version_bottom_layer['message']}'
        return self.__handle_error(message)

    def __check_geometry(self, both_layers_differences):
        layers_path = os.path.join(self.tmp_files_path, 'layers')
        gpkg_driver = ogr.GetDriverByName("GPKG")

        file_name_and_path_top_layer = os.path.join(layers_path, f'layer_{self.top_layer_id}.gpkg')
        top_layer = gpkg_driver.Open(file_name_and_path_top_layer, 1)
        top_layer_geometry = top_layer.GetLayer()

        file_name_and_path_bottom_layer = os.path.join(layers_path, f'layer_{self.bottom_layer_id}.gpkg')
        bottom_layer = gpkg_driver.Open(file_name_and_path_bottom_layer, 1)
        bottom_layer_geometry = bottom_layer.GetLayer()

        if top_layer is None or bottom_layer is None:
            print ("ERROR: open GPKG file failed")
            return

        if (self.geofence_mode == 'point-polygon'):
            for item in both_layers_differences:
                if item['layer_id'] == self.top_layer_id:
                    if ('geom' in item):
                        wkb_data = base64.b64decode(item['geom'])
                        point = ogr.CreateGeometryFromWkb(wkb_data)

                    if (item['action'] == 'feature.create'):
                        out_feature = ogr.Feature(top_layer_geometry.GetLayerDefn())
                        out_feature.SetGeometry(point)
                        out_feature.SetFID(item['fid'])
                        top_layer_geometry.CreateFeature(out_feature)
                        out_feature = None
                    elif (item['action'] == 'feature.delete'):
                        top_layer_geometry.DeleteFeature(item['fid'])
                        return
                    elif (item['action'] == 'feature.update'):
                        feature = top_layer_geometry.GetFeature(item['fid'])
                        feature.SetGeometry(point)
                        top_layer_geometry.SetFeature(feature)

                    bottom_layer_geometry.ResetReading()

                    for polygon_feature in bottom_layer_geometry:
                        polygon_geom = polygon_feature.GetGeometryRef()
                        if polygon_geom.Contains(point):
                            message = f"Точка с id {item['fid']} попала в границы полигона с id {polygon_feature.GetFID()}"
                            self.send_message(message)
                elif item['layer_id'] == self.bottom_layer_id:
                    if ('geom' in item):
                        wkb_data = base64.b64decode(item['geom'])
                        polygon = ogr.CreateGeometryFromWkb(wkb_data)

                    if (item['action'] == 'feature.create'):
                        out_feature = ogr.Feature(bottom_layer_geometry.GetLayerDefn())
                        out_feature.SetGeometry(polygon)
                        out_feature.SetFID(item['fid'])
                        bottom_layer_geometry.CreateFeature(out_feature)
                        out_feature = None
                    elif (item['action'] == 'feature.delete'):
                        bottom_layer_geometry.DeleteFeature(item['fid'])
                        return
                    elif (item['action'] == 'feature.update'):
                        feature = bottom_layer_geometry.GetFeature(item['fid'])
                        feature.SetGeometry(polygon)
                        bottom_layer_geometry.SetFeature(feature)

                    top_layer_geometry.ResetReading()

                    for point_feature in top_layer_geometry:
                        point_geom = point_feature.GetGeometryRef()
                        if point_geom is not None and polygon.Contains(point_geom):
                            message = f"Точка с id {point_feature.GetFID()} попала в границы полигона с id {item['fid']}"
                            self.send_message(message) 

    def __get_latest_version_and_epoch(self, layer_id):
        """
        This function returns a dict with the last version and epoch of the layer


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
            if not versioning_status: message = f'Versioning for layer with id {layer_id} is turned off'
            else: return {'status':'ok', 'version': versioning_info['latest'], 'epoch': versioning_info['epoch']}
        else: message = f'Request error when getting version and epoch for the layer with id {layer_id} from the server: {layer_info.status_code}'
        return self.__handle_error(message)
    
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

        Returns
        -------
        dict
            status key contains error or ok, if error then message key contains explanations, if ok then dif_list key contains the list of updated features
        """
        req = f'{self.ngw_host}/api/resource/{layer_id}/feature/changes/check?epoch={epoch}&initial={previous_version}&target={latest_version}&geom_format=geojson'
        difference_versions_info = requests.get(req, auth = (self.ngw_login, self.ngw_password))
        
        if difference_versions_info.status_code == 200:
            fetch = difference_versions_info.json()['fetch']
            if __debug__:
                print(f'Request link for layer with id {layer_id} between versions {previous_version} and {latest_version}: {req}')
                print(f'Link for more information: {fetch}')
            result = requests.get(fetch, auth = (self.ngw_login, self.ngw_password))
            if result.status_code == 200:
                json_result = [item for item in result.json() if "vid" in item] 

                requests_for_versions = self.__get_versions_information(layer_id, previous_version, latest_version)
                if requests_for_versions['status'] == 'ok':
                    time_for_versions = requests_for_versions['answer']
                    for item in json_result:
                        if 'vid' in item:
                            for time in time_for_versions:
                                if 'id' in time:
                                    if time['id'] == item['vid']:
                                        item['time'] = time['tstamp']
                                        item['layer_id'] = layer_id

                    sorted_by_time_json = sorted(json_result, key=self.__get_time)
                    if __debug__:
                        print("UPDATE JSON WITH TIME:", sorted_by_time_json, "\n")
                                    
                    return {'status':'ok', 'dif_list':sorted_by_time_json}
                else: message = requests_for_versions['message']
            else: message = f"Error when getting details about changes for the layer {layer_id} between versions {previous_version} and {latest_version} for epoch {epoch}"
        else: message = f"Error when getting the list of features for the layer {layer_id} between versions {previous_version} and {latest_version} for epoch {epoch}"
        return self.__handle_error(message)

    def __get_versions_information(self, layer_id, last_version, latest_version):
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
        for version in range(last_version, latest_version+1):
            req = f'{self.ngw_host}/api/resource/{layer_id}/feature/version/{version}'
            current_version_info = requests.get(req, auth = (self.ngw_login, self.ngw_password))
            if current_version_info.status_code == 200:
                answer.append(current_version_info.json())
            else:
                if __debug__:
                    print(f"Error while getting feature of version {version} for the layer with id {layer_id}\n")
        return {'status':'ok', 'answer':answer}

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
            data_file_name_and_path = os.path.join(self.tmp_files_path, self.DATA_FILE_NAME)
            with open(data_file_name_and_path, 'r') as data_file:
                data = json.load(data_file)

            for layer in data.values():
                if layer['id'] == layer_id:
                    return {'status':'ok', 'version':layer['version'], 'epoch':layer['epoch']}

            message = f'Error when finding local info for the layer with id: {layer_id}'
        except FileNotFoundError:
            message = f"Error: File '{data_file_name_and_path}' not found."
        except json.JSONDecodeError:
            message = f"Error: File '{data_file_name_and_path}' contains invalid JSON."
        except Exception as e:
            message = f"Error when opening the file '{data_file_name_and_path}': {e}"
        
        return self.__handle_error(message)

    def __handle_error(self, message):
        """
        This function returns a dict with status code and message with more information about error. It was made to optimise other functions


        Parametrs
        ---------
        message : str
            onfo about error

        Returns
        -------
        dict
            status key contains error and message key contains explanations
        """
        if __debug__:
            print(message, '\n')
        return {'status':'error', 'message':message}

    def __get_time(self, item):
        if 'time' in item:
            return datetime.fromisoformat(item['time'])
        else: return datetime.min

def main(config_path):
    new_lph = NGWGeofencer(config_path)
    new_lph.run_script()

if __name__ == '__main__':
    try:
        import argparse
        parser = argparse.ArgumentParser(description='Checking the configuration file')
        parser.add_argument('--config_file', metavar='path', required=True,
                        help='the path to config file')
        args = parser.parse_args()
        main(args.config_file)
    except ErrorConnection as e:
        print(e)