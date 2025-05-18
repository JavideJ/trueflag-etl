import json
from etl.transform import tweet_cleaning, yt_comment_cleaning
from etl.utils import print_error
from datetime import datetime
import logging
from etl.load import get_schema

class SkipFolderException(Exception):
    """Excepción para indicar que se debe saltar una carpeta."""
    pass

def get_folder_path(date_to_upload, folder):
    """
    Construye el path a la carpeta sobre la que se quiere iterar.
    
    Parámetros:"""
    if date_to_upload == 'today':   #idealmente entraría en este if
        today = datetime.today()
        day_to_upload = today.day
        if len(str(day_to_upload))==1:
            month_to_upload = f'0{day_to_upload}'
        month_to_upload = today.month
        if len(str(month_to_upload))==1:
            month_to_upload = f'0{month_to_upload}'
        year_to_upload = today.year
        date_to_upload = f'{year_to_upload}/{month_to_upload}/{day_to_upload}'

        folder_path = f'{folder}/{date_to_upload}/'

    elif date_to_upload == 'all':
        folder_path = f'{folder}/'
    else:
        folder_path = f'{folder}/{date_to_upload}/'
    return folder_path

def check_json_vs_schema(json_obj, schema):
    """
    Comprobación de que el json contiene los campos esperados.
    
    Parámetros:
    - json_obj: dict
    - schema: bigquery.SchemaField
    """
    # Se comprueba solo el primer nivel
    fields_1 = set([field.name for field in schema])
    json_keys_1 = set(json_obj.keys())

    extra_keys  = json_keys_1 - fields_1
    missing_keys  = fields_1 - json_keys_1

    if len(extra_keys) > 0:
        msg = f'El archivo {json_obj} tiene keys nuevas {extra_keys}'
    elif len(missing_keys ) > 0:
        msg = f'Al archivo {json_obj} le faltan las keys {missing_keys}'
    else:
        msg = True
    return msg


def extract(date_to_upload, folder, bucket):
    """
    Extrae los datos del bucket de las diferentes fuentes.

    Parámetros:
    - date_to_upload: str indica la fecha de los datos a cargar. Puede ser 'all', 'today' o una fecha en formato YYYY/MM/DD
    - folder: str carpeta del bucket sobre la que iterar
    - bucket: s3 bucket
    """
    # Se construye el path al folder sobre el que iterar
    folder_path = get_folder_path(date_to_upload, folder)
    all_data, id_list, date_list = [], [], []

    # Itera sobre los json de una fuente determinada
    for obj in bucket.objects.filter(Prefix=folder_path):
        if obj.key.endswith('.json'):
            try:
                body = obj.get()['Body'].read()
                json_data = json.loads(body)
            except Exception as e:
                logging.error(e)
                print_error(e)

            # Comprobación de que el id existe y no está vacío
            if ('id' in json_data.keys()) & (len(json_data['id']) > 0):
                id_list.append(json_data['id'])
            else:
                msg = f'No existe "id" en el registro {obj}'
                logging.error(msg)
                print_error(msg)
                continue

            # Limpieza según la fuente
            try:
                if 'tweet' in folder_path.lower():
                    json_data = tweet_cleaning(json_data)
                elif 'youtubecomment' in folder_path.lower():
                    json_data = yt_comment_cleaning(json_data)
                    
            except Exception as e:
                logging.error(e)
                print_error(e)

            # Comprobación de que tenemos los mismos campos del schema de BQ
            schema = get_schema(folder.lower())
            check_keys = check_json_vs_schema(json_data, schema)
            if check_keys:
                pass
            else:
                logging.error(check_keys)
                print_error(check_keys)
                continue

            # Más adelante nos hará falta una lista de fechas
            date = json_data['date'].split('T')[0]
            if date not in date_list:
                date_list.append(date)

            all_data.append(json_data)

    if len(all_data) == 0:
        msg = f'No hay archivos en la ruta {folder_path}\n'
        logging.info(msg)
        raise SkipFolderException(f'{msg}')
    else:
        msg = f'{len(all_data)} archivos extraídos'
        logging.info(msg)
        print(msg)

    return all_data, id_list, date_list
