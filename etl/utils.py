import boto3
from botocore.handlers import disable_signing
import yaml
import os
from datetime import datetime
import logging
import re

def load_yaml_to_dict(filepath):
    """
    Carga un archivo yaml y lo convierte en un diccionario.
    
    Parámetros:
    - filepath: str path al archivo yaml
    """
    with open(filepath, 'r') as file:
        data = yaml.safe_load(file)
    return data

def connect_s3(bucket_name):
    """
    Se conecta a un bucket de s3.
    """
    try:
        s3 = boto3.resource('s3')
        s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
        bucket = s3.Bucket(bucket_name)
    except Exception as e:
        logging.critical(e)
        raise ConnectionError(e)

    return bucket

def bq_connect(credentials):
    """
    Crea el cliente de BQ.

    Parámetros:
    - credentials: str path al json con las credenciales de la cuenta de servicio
    """
    try:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials
        bq_client = bigquery.Client()
    except Exception as e:
        logging.critical(e)
        print_error(e)

    return bq_client

def get_date_to_upload(date_to_upload):
    """
    Obtiene el día, mesla fecha que queremos cargar.
    
    Parámetros:
    - date_to_upload: str es la fecha que queremos cargar, definida en el config.yaml, idealmente será el día actual
    """
    if date_to_upload == 'today':   #idealmente entraría en este if, pero para este ejemplo cargaremos la fecha que tienen los datos en el bucket
        today = datetime.datetime.today()
        day_to_upload = today.day
        month_to_upload = today.month
        year_to_upload = today.year
        date_to_upload = f'{year_to_upload/month_to_upload/day_to_upload}'

    return date_to_upload

def print_success(message):
    """
    Imprime un mensaje en color verde.

    Parámetros:
    - message: str con el mensaje
    """
    
    print("\033[92m{}\033[00m".format(message)) 


def print_error(message):
    """
    Imprime un mensaje en color rojo.

    Parámetros:
    - message: str con el mensaje
    """
    
    print("\033[91m{}\033[00m".format(message))

def check_yaml_vars(yaml_vars):
    """
    Hace varias comprobaciones sobre las variables del config.yaml

    Parámetros:
    - yaml_vars: dict variables del config.yaml
    """
    # Revisión de que tenemos las variables correctamente
    for i in yaml_vars:
        for var in yaml_vars[i]:
            value = yaml_vars[i][var]

            if value is None:
                msg = f'La variable "{var}" no puede estar vacía. Revisa el config.yaml'
                logging.critical(msg)
                raise ValueError(msg)
            
    # Asegurarse que los nombres de los folders están bien escritos
    allowed_folders = ['Tweet', 'YoutubeComment']
    folders = yaml_vars['bucket']['folders']
    invalid_folders = [folder for folder in folders if folder not in allowed_folders]
    yaml_vars['bucket']['folders'] = [folder for folder in folders if folder in allowed_folders]

    for folder in invalid_folders:
        msg = f'La fuente de datos {folder} no existe o no está contemplada.'
        logging.warning(msg)
        print_error(msg)

    # Comprobación del formato de date_to_upload
    date_to_upload = yaml_vars['env-vars']['date_to_upload']
    pattern = r'^(today|all|\d{4}/\d{2}/\d{2})$'

    if re.fullmatch(pattern, date_to_upload):
        pass
    else:
        msg = f'El formato de la variable "date_to_upload" no es válido, revisa el config.yaml'
        logging.critical(msg)
        raise ValueError(msg)

    return yaml_vars
