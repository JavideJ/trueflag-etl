from google.cloud import bigquery
import logging
from etl.utils import print_error, print_success

def get_schema(dataset_id):
    """
    Elige el esquema de la tabla de BQ según la fuente de datos que se vaya a cargar.
    
    Parámetros:
    - dataset_id: str dataset de BQ
    """
    if dataset_id=='tweet':
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
    
    elif dataset_id=='youtubecomment':

        schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sentiment", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "categories", "RECORD", mode="REPEATED",
                fields=[
                    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "feed", "RECORD", mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("date", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("msgId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(
                "user", "RECORD", mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("username", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField("link", "STRING", mode="NULLABLE"),
        ]
        return schema

    else:
        msg = f'No se ha definido ningún esquema para {dataset_id}'
        logging.critical(msg)
        raise(msg)

def get_table_id(dataset_id, yaml_vars):
    """
    Según la fuente de datos que estemos cargando, elige la tabla de destino.

    Parámetros:
    - dataset_id: str dataset de BQ
    - yaml_vars: dict variables del config.yaml
    """
    if dataset_id=='tweet':
        table_id = yaml_vars['bigquery']['raw_tweet_table']
    elif dataset_id=='youtubecomment':
        table_id = yaml_vars['bigquery']['raw_yt_comment_table']
    else:
        msg = f'No se ha definido ningún table_id para {dataset_id}. Revisar load.py'
        logging.critical(msg)
        raise(msg)
    return table_id

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
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"
    table_ref = bigquery.TableReference.from_string(table_full_id)
    write_disposition = 'WRITE_APPEND'

    # Comprobar si la tabla existe
    try:
        bq_client.get_table(table_ref)
        table_exists = True
    except:
        table_exists = False

    # Crear la tabla si no existe
    if not table_exists:
        try:
            table = bigquery.Table(table_ref, schema=schema)
            
            # Se particiona por la columna "date"
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="date",
            )

            # Se clusteriza por sentiment
            table.clustering_fields = ["sentiment"]  

            bq_client.create_table(table)

        except Exception as e:
            msg = f'No se ha podido crear la tabla {table_full_id}: {e}'
            logging.error(msg)
            print_error(msg)

    # Subir el DataFrame a BigQuery con el esquema definido
    try:
        job_config = bigquery.LoadJobConfig(schema=schema,
                                            autodetect=False,
                                            write_disposition=write_disposition) #Añadimos las nuevas filas
        job = bq_client.load_table_from_json(data, table_ref, job_config=job_config)
        job.result()
        msg = f'{len(data)} registros subidos a {table_ref}'
        print_success(msg)
        logging.info(msg)
    except Exception as e:
        msg = f"Error al cargar datos en BigQuery: {e}"
        logging.error(msg)
        print_error(msg)

def check_unique(all_data, check_list, bq_client, col_to_check, date_list, project_id, dataset_id, table_id):
    """
    Para evitar duplicidades, chequea si los valores de una lista ya existen en BBDD.
    
    Parámetros:
    - all_data: : list con los jsons a subir
    - check_list: list valores a comprobar si ya existen en BQ
    - bq_client: google.cloud.bigquery.client.Client
    - col_to_check: str columna a comprobar su unicidad
    - date_list: list fechas de los datos que se están subiendo
    - project_id: str proyecto de BQ
    - dataset_id: str dataset en BQ
    - table_id: str tabla en BQ
    """
    # Arreglamos el formato de la fecha para la query
    date_list_str = [date.replace('/', '-') for date in date_list]
    date_list_str = ['TIMESTAMP("' + i + '")' for i in date_list_str]
    date_list_str = ', '.join(date_list_str)
        
    # Lanzamos la query para obtener los ids que ya existen en BQ
    query = f"""
    SELECT {col_to_check}
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE TIMESTAMP_TRUNC(date, DAY) IN ({date_list_str})
    """
    try:
        query_job = bq_client.query(query)
        results = query_job.result()
    except Exception as e:
        logging.critical(e)
        raise(e)
    
    id_set = {row[0] for row in results}

    # Nos quedamos con los registros cuyos ids no están en BQ
    upload_set = set(check_list) - id_set
    data_to_upload = [i for i in all_data if i[col_to_check] in upload_set]
    return data_to_upload

def daily_cat_table(project_id, dataset_id, table_id_raw, bq_client):
    """
    Crea una tabla en BQ con el número de categorías por día.

    Parámetros:
    - project_id: str proyecto de BQ
    - dataset_id: str dataset en BQ
    - table_id_raw: str tabla raw en BQ
    - bq_client: google.cloud.bigquery.client.Client
    """
    # Se obtiene el table_id según la fuente de datos a cargar
    if dataset_id=='tweet':
        full_table_id  = f'{project_id}.{dataset_id}.daily_cat_tweet'
        full_table_id_raw = f'{project_id}.{dataset_id}.{table_id_raw}'
    elif dataset_id=='youtubecomment':
        full_table_id  = f'{project_id}.{dataset_id}.daily_cat_youtubecomment'
        full_table_id_raw = f'{project_id}.{dataset_id}.{table_id_raw}'

    # Agregación: num categorías por día
    query = f"""
    CREATE OR REPLACE TABLE `{full_table_id}` 
    CLUSTER BY date
    AS
    SELECT
    DATE(date) AS date,
    category.name AS category_name,
    COUNT(DISTINCT t.id) AS count
    FROM
    `{full_table_id_raw}` t,
    UNNEST(t.categories) AS category
    GROUP BY
    date,
    category_name
    """
    try:
        query_job = bq_client.query(query)
        query_job.result()

        msg = f'Tabla {full_table_id} actualizada'
        logging.info(msg)
        print(msg)
    except Exception as e:
        logging.error(e)
        print_error(e)

def daily_country_table(project_id, dataset_id, table_id_raw, bq_client):
    """
    Crea una tabla en BQ con el número de países por día.

    Parámetros:
    - project_id: str proyecto de BQ
    - dataset_id: str dataset en BQ
    - table_id_raw: str tabla raw en BQ
    - bq_client: google.cloud.bigquery.client.Client
    """
    # Se obtiene el table_id según la fuente de datos a cargar
    if dataset_id=='tweet':
        full_table_id  = f'{project_id}.{dataset_id}.daily_country_tweet'
        full_table_id_raw = f'{project_id}.{dataset_id}.{table_id_raw}'
    elif dataset_id=='youtubecomment':
        return

    # Agregación: num categorías por día
    query = f"""
    CREATE OR REPLACE TABLE `{full_table_id}`
    CLUSTER BY date
    AS
    SELECT
    DATE(date) AS date,
    user.location.country AS country,
    COUNT(DISTINCT id) AS count
    FROM
    `{full_table_id_raw}`
    GROUP BY
    date,
    country
    """
    try:
        query_job = bq_client.query(query)
        query_job.result()

        msg = f'Tabla {full_table_id} actualizada'
        logging.info(msg)
        print(msg)
    except Exception as e:
        logging.error(e)
        print_error(e)

def daily_sentiment_table(project_id, dataset_id, table_id_raw, bq_client):
    """
    Crea una tabla en BQ con el número de sentimientos por día.

    Parámetros:
    - project_id: str proyecto de BQ
    - dataset_id: str dataset en BQ
    - table_id_raw: str tabla raw en BQ
    - bq_client: google.cloud.bigquery.client.Client
    """
    # Se obtiene el table_id según la fuente de datos a cargar
    if dataset_id=='tweet':
        full_table_id  = f'{project_id}.{dataset_id}.daily_sentiment_tweet'
        full_table_id_raw = f'{project_id}.{dataset_id}.{table_id_raw}'
    elif dataset_id=='youtubecomment':
        full_table_id  = f'{project_id}.{dataset_id}.daily_sentimentyoutubecomment'
        full_table_id_raw = f'{project_id}.{dataset_id}.{table_id_raw}'

    # Agregación: num categorías por día
    query = f"""
    CREATE OR REPLACE TABLE `{full_table_id}`
    CLUSTER BY date
    AS
    SELECT
    DATE(date) AS date,
    sentiment,
    COUNT(DISTINCT id) AS count
    FROM
    `{full_table_id_raw}`
    GROUP BY
    date,
    sentiment
    """
    try:
        query_job = bq_client.query(query)
        query_job.result()

        msg = f'Tabla {full_table_id} actualizada'
        logging.info(msg)
        print(msg)
    except Exception as e:
        logging.error(e)
        print_error(e)

def aggregated_tables(project_id, dataset_id, table_id_raw, bq_client):
    """
    Lanza varias queries para obtener resultados agregados por día.
    
    Parámetros:
    - project_id: str proyecto de BQ
    - dataset_id: str dataset en BQ
    - table_id_raw: str tabla raw en BQ
    - bq_client: google.cloud.bigquery.client.Client
    """
    # Tabla de recuento de categorías por día
    try:
        daily_cat_table(project_id, dataset_id, table_id_raw, bq_client)
    except Exception as e:
        logging.error(e)
        print_error(e)
    # Tabla de recuento de países por día
    try:
        daily_country_table(project_id, dataset_id, table_id_raw, bq_client)
    except Exception as e:
        logging.error(e)
        print_error(e)
    # Tabla de recuento de sentimiento por día
    try:
        daily_sentiment_table(project_id, dataset_id, table_id_raw, bq_client)
    except Exception as e:
        logging.error(e)
        print_error(e)
