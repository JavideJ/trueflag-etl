import boto3
from botocore.handlers import disable_signing
import yaml
import json
from datetime import datetime

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
    # client = boto3.client('s3')
    # client.meta.events.register('choose-signer.s3.*', disable_signing)
    s3 = boto3.resource('s3')
    s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
    bucket = s3.Bucket(bucket_name)

    return bucket

def list_prefixes(client, bucket_name, prefix):
    response = client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix,
        Delimiter='/'
    )
    return [cp['Prefix'] for cp in response.get('CommonPrefixes', [])]

def list_objects(client, bucket_name, prefix):
    """Lista archivos dentro de un prefijo (sin agrupar por carpeta)"""
    response = client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix
    )
    return response.get('Contents', [])

def read_json_object(client, bucket_name, key):
    obj = client.get_object(Bucket=bucket_name, Key=key)
    content = obj['Body'].read()
    return json.loads(content)

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

