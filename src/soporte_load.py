def guardar_datos(hoteles_mergeo, ruta_archivo="..data/data_transform/reservas_hoteles_limpio.pkl"):
    """Renombra columnas y guarda el DataFrame en un archivo pickle."""
    hoteles_mergeo.rename(columns={'ciudad': 'nombre_ciudad', 'valoración': 'valoracion'}, inplace=True)
    hoteles_mergeo.to_pickle(ruta_archivo)

def guardar_eventos(df_eventos, ruta_archivo="../data/data_raw/eventos_madrid.pkl"):
    """Guarda el DataFrame de eventos en un archivo pickle."""
    df_eventos.to_pickle(ruta_archivo)

def guardar_hoteles_competencia(df_hoteles, ruta_archivo="../data/data_raw/hoteles_competencia.csv"):
    """Guarda el DataFrame de hoteles de competencia en un archivo CSV."""
    df_hoteles.to_csv(ruta_archivo, index=False, encoding="utf-8")