# Imports
import json
from google.oauth2 import service_account
from google.cloud import secretmanager
import pydata_google_auth

###########################################################
#
#       FUNCION GET_CREDENTIALS
#
###########################################################

def get_credentials(type):
    '''
        Recive el tipo de credencial requerida: BQ o Sheets
        Retorna las credenciales para el acceso
    '''

    # Datos generales
    scopes = ['https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/drive']
    version = 'latest'
    secret = 'growth-automation-key'
    project = '868203979621'

    # Login web to authenticate my user
    credentials_user = pydata_google_auth.get_user_credentials(scopes, auth_local_webserver=False)
    # Create the Secret Manager Client
    client = secretmanager.SecretManagerServiceClient(credentials=credentials_user)
    # Build the resource name of the secret version
    name = f'projects/{project}/secrets/{secret}/versions/{version}'
    # Access the secret version
    response = client.access_secret_version(name=name)

    if type not in ['BQ', 'Sheets']:
        print('Error en typo de Credencial enviada')
        cred = '-'
    elif type == 'BQ':
        cred = service_account.Credentials.from_service_account_info(json.loads(response.payload.data.decode('UTF-8')))
    else:
        cred = json.loads(response.payload.data.decode('UTF-8'))
    return cred
