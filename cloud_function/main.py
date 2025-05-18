from etl.utils import connect_s3, bq_connect, load_yaml_to_dict, check_yaml_vars
from etl.load import upload_raw_data, check_unique, get_table_id, aggregated_tables
from etl.extract import extract, SkipFolderException
import logging
import functions_framework

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='project.log',
    filemode='a'
)

@functions_framework.http
def main(request):
    # Se obtienen las variables guardadas en el config.yaml y comprobación de que están bien definidas
    yaml_vars = load_yaml_to_dict('config.yaml')
    yaml_vars = check_yaml_vars(yaml_vars)        

    project_id = yaml_vars['env-vars']['project_id']
    bucket_name = yaml_vars['bucket']['bucket_name']
    folders = yaml_vars['bucket']['folders']
    date_to_upload = yaml_vars['env-vars']['date_to_upload']
    credentials = yaml_vars['env-vars']['credentials']

    # Conexión al bucket de s3
    bucket = connect_s3(bucket_name)

    # Iteramos sobre cada carpeta del bucket para la cual se tenga la ETL lista
    for folder in folders:
        print(f'Extrayendo datos de la fuente {folder}')
        try:
            all_data, check_list, date_list = extract(date_to_upload, folder, bucket)
        except SkipFolderException as e:
            print(e)
            continue

        # Cliente de BQ y variables
        bq_client = bq_connect(credentials)
        col_to_check='id'
        dataset_id = folder.lower()
        table_id_raw = get_table_id(dataset_id, yaml_vars)

        # Subimos los datos a BQ. Subimos sólo los registros nuevos, si los hay, para garantizar la idempotencia del sistema
        data_to_upload = check_unique(all_data, check_list, bq_client, col_to_check, date_list, project_id, dataset_id, table_id_raw)
        if len(data_to_upload)==0:
            msg = 'Todos los registros a cargar ya existen en BQ'
            print(msg)
            logging.info(msg)
        else:
            upload_raw_data(data_to_upload, project_id, dataset_id, table_id_raw, bq_client)
            aggregated_tables(project_id, dataset_id, table_id_raw, bq_client)

    return 'Ejecución finalizada'
