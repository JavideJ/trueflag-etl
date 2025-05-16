from google.cloud import bigquery
import os

def get_schema(folder):
    """
    Elige el esquema de la tabla de BQ según la fuente de datos que se vaya a cargar.
    
    Parámetros:
    - folder: str nombre de la carpeta en el bucket de s3
    """
    if folder.lower()=='tweet':
        schema = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("sentiment", "STRING"),
    bigquery.SchemaField("categories", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
    ]),
    bigquery.SchemaField("feed", "RECORD", fields=[
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
    ]),
    bigquery.SchemaField("date", "TIMESTAMP"),
    bigquery.SchemaField("msgId", "STRING"),
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("media", "STRING"),
    bigquery.SchemaField("text", "STRING"),
    bigquery.SchemaField("user", "RECORD", fields=[
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("username", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("followers", "INTEGER"),
        bigquery.SchemaField("friends", "INTEGER"),
        bigquery.SchemaField("gender", "STRING"),
        bigquery.SchemaField("location", "RECORD", fields=[
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("subregion", "STRING"),
        ]),
    ]),
    bigquery.SchemaField("link", "STRING"),
    bigquery.SchemaField("parentId", "STRING")
]

        return schema

def bq_connect():
    """
    Crea el cliente de BQ.
    """
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'cred.json'
    bq_client = bigquery.Client()
    return bq_client

def upload_raw_data(data, project_id, dataset_id, table_id, bq_client):
    """
    Sube los datos a BQ. Se crea la tabla antes si no existe.

    Parámetros:
    - data: list con los jsons a subir
    - project_id: str proyecto de BQ
    - dataset_id: str dataset en BQ
    - table_id: str tabla en BQ
    - bq_client: google.cloud.bigquery.client.Client
    """
    # Seleccionamos el esquema de BQ
    schema = get_schema(dataset_id)

    # Construir la referencia completa de la tabla
    table_ref = bigquery.TableReference.from_string(f"{project_id}.{dataset_id.lower()}.{table_id}")
    write_disposition = 'WRITE_APPEND'

    # Comprobar si la tabla existe
    try:
        bq_client.get_table(table_ref)
        table_exists = True
    except:
        table_exists = False

    # Crear la tabla si no existe
    if not table_exists:
        # try:
        table = bigquery.Table(table_ref, schema=schema)
        
        # Se particiona por la columna "date"
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
        )

        # Se clusteriza por "sentiment" y "user.id"
        table.clustering_fields = ["sentiment"]  

        bq_client.create_table(table)

        # except Exception as e:
        #     print(f"Error al crear la tabla {table_ref}: {e}")

    # Subir el DataFrame a BigQuery con el esquema definido
    try:
        job_config = bigquery.LoadJobConfig(schema=schema,
                                            autodetect=False,
                                            write_disposition=write_disposition) #Añadimos las nuevas filas
        job = bq_client.load_table_from_json(data, table_ref, job_config=job_config)
        job.result()
        print(f'{len(data)} registros subidos a {table_ref}')
    except Exception as e:
        print(f"Error al cargar datos en BigQuery: {e}")

def check_unique(all_data, check_list, bq_client, col_to_check, date_to_upload, project_id, dataset_id, table_id):
    """
    Para evitar duplicidades, chequea si los valores de una lista ya existen en BBDD.
    
    Parámetros:
    - all_data: : list con los jsons a subir
    - check_list: list valores a comprobar si ya existen en BQ
    - bq_client: google.cloud.bigquery.client.Client
    - col_to_check: str columna a comprobar su unicidad
    - date_to_upload: str fecha de los datos que se están subiendo
    - project_id: str proyecto de BQ
    - dataset_id: str dataset en BQ
    - table_id: str tabla en BQ
    """
    # Lanzamos la query para obtener los ids que ya existen en BQ
    date_to_upload = date_to_upload.replace('/', '-')
    query = f"""
    SELECT {col_to_check}
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE TIMESTAMP_TRUNC(date, DAY) = TIMESTAMP("{date_to_upload}")
    """
    query_job = bq_client.query(query)
    results = query_job.result()
    id_set = {row[0] for row in results}

    # Nos quedamos con los registros cuyos ids no están en BQ
    upload_set = set(check_list) - id_set
    data_to_upload = [i for i in all_data if i[col_to_check] in upload_set]
    return data_to_upload
