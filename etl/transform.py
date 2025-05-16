def tweet_cleaning(json_data):
    """
    Realiza una determinada limpieza sobre un campo del diccionario.

    Parámetros:
    - json_data: json
    """
    for k in json_data['categories']:
        k['name'] = k['name'].strip('[]')
    return json_data