import json
from etl.transform import tweet_cleaning
def extract(folder_path, bucket):
    """
    Extrae los datos del bucket de las diferentes fuentes.

    Parámetros:
    - folder_path: str path de cada fuente y fecha sobre el que iterar
    - bucket: s3 bucket
    """
    # Itera sobre los json de una fuente y una fecha determinada
    all_data, id_list = [], []
    print(f'Extrayendo los archivos de {folder_path}...')
    for obj in bucket.objects.filter(Prefix=folder_path):
        if obj.key.endswith('.json'):
            body = obj.get()['Body'].read()
            json_data = json.loads(body)
            # Añadimos las columnas que a veces faltan
            json_data.setdefault('media', None)
            json_data.setdefault('parentId', None)
            # Limpieza para TWEET
            if 'tweet' in folder_path.lower():
                json_data = tweet_cleaning(json_data)
                id_list.append(json_data['id'])

            all_data.append(json_data)
    print(f'{len(all_data)} archivos extraídos')

    return all_data, id_list