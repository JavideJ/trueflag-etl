from etl.utils import connect_s3, load_yaml_to_dict, list_prefixes, list_objects, read_json_object, get_date_to_upload
from etl.load import upload_raw_data, bq_connect, check_unique
from etl.extract import extract
import pandas as pd
import json
import pickle

def main():
    # Se obtienen las variables guardadas en el config.yaml
    yaml_vars = load_yaml_to_dict('config.yaml')

    # Conexión al bucket de s3
    bucket_name = yaml_vars['bucket']['bucket_name']
    bucket = connect_s3(bucket_name)

    # Variables que nos harán falta
    folders = yaml_vars['bucket']['folders']
    date_to_upload_yaml = yaml_vars['env-vars']['date_to_upload']

    date_to_upload = get_date_to_upload(date_to_upload_yaml)

    # Iteramos sobre cada carpeta del bucket para la cual se tenga la ETL lista. Se hacen cargas de datos por día
    for folder in folders:
        print(f'Cargando datos de la fuente {folder}')
        folder_path = f'{folder}/{date_to_upload}/'
        all_data, check_list = extract(folder_path, bucket)

        # with open('data/check_list.pickle', 'wb') as handle:
        #     pickle.dump(check_list, handle, protocol=pickle.HIGHEST_PROTOCOL)
        # with open('data/all_data_tweet.pickle', 'rb') as handle:
        #     all_data = pickle.load(handle)
        # with open('data/check_list.pickle', 'rb') as handle:
        #     check_list = pickle.load(handle)           

        # Subimos el df a BQ
        project_id = yaml_vars['env-vars']['project_id']
        table_id = yaml_vars['bigquery']['raw_tweet_table']

        bq_client = bq_connect()
        # Subimos sólo los registros nuevos, si los hay, para garantizar la idempotencia del sistema
        col_to_check='id'
        dataset_id = folder.lower()
        data_to_upload = check_unique(all_data, check_list, bq_client, col_to_check, date_to_upload, project_id, dataset_id, table_id)

        if len(data_to_upload)==0:
            print('Todos los registros a cargar ya existen en BQ')
            return
        else:
            upload_raw_data(data_to_upload, project_id, dataset_id, table_id, bq_client)


if __name__ == "__main__":
    main()
