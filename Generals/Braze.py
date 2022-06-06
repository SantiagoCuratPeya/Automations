# Imports
from math import ceil, isnan
from datetime import datetime, timedelta
import requests, json

###########################################################
#
#       CREATE PROPERTIES
#
###########################################################

def create_prop(df, base):
    '''
        Esta funcion recibe 2 parametros y devuelve un diccionario de propiedades:
        . DF con los Ids y las propiedades
        . Nombre de la columna que contiene los Ids
        En caso de errores:
        . Devuelve vacio y muestra el mensaje de 'Error'
    '''
    
    try:
        ids = df[base].tolist()
        prop = {}
        if len(df.columns) > 1:
            cols = [i for i in df.columns if i not in base]
            for i in ids:
                p = {}
                for j in cols:
                    try:
                        if not isnan(float(df[df[base] == i][j].values[0])):
                            p[j] = float(df[df[base] == i][j].values[0])
                    except:
                        if not isnan(df[df[base] == i][j].values[0]):
                            p[j] = df[df[base] == i][j].values[0]
                if p:
                    prop[i] = p
        return prop
    except:
        print('Error')
        return 

###########################################################
#
#       FUNCION BATCH USERS
#
###########################################################

def batch_users(ids, name, prop):
    '''
        Esta funcion recibe 3 parametros y devuelve una lista de listas:
        . Listado de Ids a ejecutar
        . Nombre del evento a colocarles
        . Diccionario de propiedades con id como key
        En caso de errores:
        . Devuelve el mensaje de 'Error'
    '''
    
    now = datetime.utcnow().isoformat()
    try:
        ids_len = len(ids)
        user_gen_list = []
        user_list = []
        dict_users = {}
        cont = 0
        cont_len = 0
        for i in ids:
            cont_len += 1
            if cont <= 74:
                if i in prop:
                    user_list.append({"external_id": str(i), "name": name, "time": "{0}".format(now), "properties": prop[i]})
                else:
                    user_list.append({"external_id": str(i), "name": name, "time": "{0}".format(now)})
                if cont == 74 or cont_len == ids_len:
                    dict_users["events"] = user_list
                    user_gen_list.append(json.dumps(dict_users))
                    user_list = []
                    dict_users = {}
                    cont = 0
                else:
                    cont += 1
        return user_gen_list
    except:
        return 'Error'
    
###########################################################
#
#       FUNCION UPLOAD BRAZE
#
###########################################################

def upload_braze(data, api_key, api_url):
    '''
        Esta funcion recibe 3 parametros y genera el Upload en Braze:
        . La data resultado de batch_users
        . La api_key
        . La api_url
        Devuelve el mensaje de 'Carga Correcta' en caso de success
        En caso de errores:
        . Devuelve el mensaje de 'Error en Carga'
    '''
    
    header = {"Content-Type": "application/json",
              "Authorization": "Bearer {0}".format(api_key),
              "X-Braze-Bulk": "true"} 
    try:
        for i in data:
            response_decoded_json = requests.post(api_url, headers=header, data=str(i))
            response_json = response_decoded_json.json()
        return 'Carga Correcta'
    except:
        return 'Error en Carga'