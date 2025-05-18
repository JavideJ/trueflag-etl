def tweet_cleaning(json_data):
    """
    Realiza una determinada limpieza sobre un campo del diccionario de tweet.

    Parámetros:
    - json_data: json
    """
    # Añadimos las columnas que a veces faltan
    json_data.setdefault('media', None)
    json_data.setdefault('parentId', None)
    # Se quitan esos corchetes que a veces hay en categories
    for k in json_data['categories']:
        k['name'] = k['name'].strip('[]')
    return json_data

def yt_comment_cleaning(json_data):
    """
    Realiza una determinada limpieza sobre un campo del diccionario de tweet.

    Parámetros:
    - json_data: json
    """
    # Se quitan esos corchetes que a veces hay en categories
    for k in json_data['categories']:
        k['name'] = k['name'].strip('[]')
    return json_data