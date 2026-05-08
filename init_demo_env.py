import geofencer.geometry_modifier as gm


def generate_config_file(ngw_host: str, ngw_login: str, ngw_password: str, top_layer_id: int, top_layer_attrs: list[str], top_layer_buffer: int, bottom_layer_id: int, bottom_layer_attrs: list[str], bottom_layer_buffer: int,
                        geofence_mode: str, tmp_file_path: str, update_period_sec: int, message_type: str, tg_user_id: int = 0):
    env_variables = {
        'NGW_HOST': ngw_host,
        'NGW_LOGIN': ngw_login,
        'NGW_PASSWORD': ngw_password,

        'TOP_LAYER_ID': top_layer_id,
        'TOP_LAYER_ATTRIBUTE_PARAMS_FOR_MESSAGE': top_layer_attrs,
        'TOP_LAYER_BUFFER': top_layer_buffer,

        'BOTTOM_LAYER_ID': bottom_layer_id,
        'BOTTOM_LAYER_ATTRIBUTE_PARAMS_FOR_MESSAGE': bottom_layer_attrs,
        'BOTTOM_LAYER_BUFFER': bottom_layer_buffer,

        'SCRIPT_PARAMETERS_GEOFENCE_MODE': geofence_mode,
        'SCRIPT_PARAMETERS_TMP_FILES_PATH': tmp_file_path,
        'SCRIPT_PARAMETERS_UPDATE_PERIOD_SEC': update_period_sec,
        'SCRIPT_PARAMETERS_MESSAGE_TYPE': message_type,

        'OPTIONAL_PARAMETERS_TG_USER_ID': tg_user_id
    }

    with open('.env', 'w', encoding='utf-8') as f:
        for key, value in env_variables.items():
            if ' ' in str(value) or any(c in str(value) for c in '=#!$%^&*()'):
                f.write(f'{key}="{value}"\n')
            else:
                f.write(f'{key}={value}\n')


if __name__ == '__main__':
    group_id = gm.get_resource_id()
    top_layer_id = gm.get_layer_id(group_id, gm.TOP_LAYER_KEYNAME, gm.POINT_LAYER_PAYLOAD, gm.INITIAL_POINT_GEOM)
    bottom_layer_id = gm.get_layer_id(group_id, gm.BOTTOM_LAYER_KEYNAME, gm.POLYGON_LAYER_PAYLOAD, gm.INITIAL_POLYGON_GEOM)

    generate_config_file(gm.BASE_URL, gm.USERNAME, gm.PASSWORD, int(top_layer_id), ["id"], 100, int(bottom_layer_id), ["id"], 200, "intersection", "./tmp/", 10, "console_message", 788612936)
