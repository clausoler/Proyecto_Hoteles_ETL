import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import re

def cargar_parquet(ruta_archivo="../data/data_raw/reservas_hoteles.parquet"):
    """Carga un archivo Parquet en un DataFrame."""
    pd.set_option('display.max_columns', None)  # Para visualizar todas las columnas
    df = pd.read_parquet(ruta_archivo, engine='pyarrow')
    return df

# Análisis fichero parquet

# Convertir columnas a formato datetime
def convertir_a_datetime(df, columnas):
    """
    Convierte las columnas especificadas a formato datetime.

    Args:
        df (pd.DataFrame): DataFrame que contiene las columnas a convertir.
        columnas (list): Lista de nombres de columnas a convertir.

    Returns:
        pd.DataFrame: DataFrame con las columnas convertidas a datetime.
    """
    for col in columnas:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    return df
 
# Análisis fichero parquet
# Tratamiento de valores nulos

def reporte_nulos(df):
    """
    Genera un reporte sobre los valores nulos de un DataFrame.

    Esta función analiza el DataFrame proporcionado y devuelve un nuevo DataFrame 
    con información detallada sobre la cantidad de valores nulos, el porcentaje 
    de valores nulos respecto al total de filas y el tipo de dato de cada columna.

    Parámetros:
    -----------
    df : pd.DataFrame
        DataFrame que se desea analizar en busca de valores nulos.

    Retorna:
    --------
    pd.DataFrame
        Un DataFrame con las siguientes columnas:
        - "número_nulos": número de valores nulos en cada columna.
        - "porcentaje_nulos": porcentaje de valores nulos respecto al total de filas.
        - "tipo_variables": tipo de dato (dtype) de cada columna.

    """
    df_reporte = pd.DataFrame()
    df_reporte["número_nulos"] = df.isnull().sum()
    df_reporte["porcentaje_nulos"] = round((df.isnull().sum() / len(df)) * 100, 2)
    df_reporte["tipo_variables"] = df.dtypes
    return df_reporte
 
def rellenar_fechas_nulas(df, columnas):
    """Rellena valores nulos de las columnas de fecha con su valor único si existe."""
    for col in columnas:
        fecha_unica = df[col].dropna().unique()[0] if df[col].notna().sum() > 0 else None
        df[col] = df[col].fillna(fecha_unica)
    return df

def eliminar_duplicados(df):
    """Elimina filas duplicadas del DataFrame."""
    df.drop_duplicates(inplace=True)
    return df
 
def filtrar_hoteles_no_competencia(df):
    """Filtra los hoteles que no son de la competencia."""
    return df[df["competencia"] == False]
     
def asignar_id_hoteles(df):
    """Asigna un nuevo ID serial basado en nombres de hotel únicos para los hoteles que no son de la competencia."""
    hotel_to_id_no_competencia = {hotel: idx + 1 for idx, hotel in enumerate(df["nombre_hotel"].unique())}
    df["id_hotel"] = df["nombre_hotel"].map(hotel_to_id_no_competencia)
    return df

def filtrar_hoteles_competencia(df):
    """Filtra los hoteles que son de la competencia."""
    return df[df["competencia"] == True]
 
def calcular_estrellas_precio(df):
    """Calcula el promedio de estrellas y precio por noche para cada hotel no competencia."""
    estrellas = df.groupby("nombre_hotel")["estrellas"].mean()
    precio = df.groupby("nombre_hotel")["precio_noche"].mean()
    return estrellas, precio
 
def analizar_precio_noche(df):
    """Analiza la columna 'precio_noche' y genera estadísticas y un boxplot si es posible."""
    if "precio_noche" in df.columns:
        df["precio_noche"] = pd.to_numeric(df["precio_noche"], errors="coerce")
        stats = df["precio_noche"].describe()
        print("\U0001F4CA Estadísticas descriptivas de 'precio_noche':\n", stats)
        num_nulos = df["precio_noche"].isnull().sum()
        print(f"\u26A0 Valores nulos en 'precio_noche': {num_nulos}")
        if df["precio_noche"].dropna().shape[0] > 0:
            plt.figure(figsize=(8, 5))
            plt.boxplot(df["precio_noche"].dropna(), vert=False, patch_artist=True)
            plt.xlabel("Precio por Noche")
            plt.title("Boxplot de Precio por Noche")
            plt.grid(True)
            plt.show()
        else:
            print("❌ No hay valores válidos en 'precio_noche'. No se puede generar el boxplot.")
    else:
        print("❌ La columna 'precio_noche' no existe en el DataFrame.")

def mergear_hoteles_precio(df_hoteles, precio):
    """Realiza un merge entre el DataFrame de hoteles y el de precios."""
    df_merged = pd.merge(df_hoteles, precio, on="nombre_hotel")
    df_merged = df_merged.drop(columns=["precio_noche_x"])
    df_merged = df_merged.rename(columns={"precio_noche_y": "precio_noche"})
    return df_merged

def cargar_hoteles_competencia(ruta_archivo="../data/data_raw/hoteles_competencia.csv"):
    """Carga el archivo CSV de hoteles de competencia."""
    return pd.read_csv(ruta_archivo, index_col=False)
 
def mergear_competencia(df_hoteles_competencia, df_competencia):
    """Realiza un merge entre el DataFrame de hoteles de competencia y el de competencia."""
    df_merged = pd.merge(df_hoteles_competencia, df_competencia, on="id_hotel")
    df_merged = df_merged.drop(columns=["precio_noche_x", "nombre_hotel_x", "estrellas_x"])
    df_merged = df_merged.rename(columns={"nombre_hotel_y": "nombre_hotel", "estrellas_y": "estrellas", "precio_noche_y": "precio_noche"})
    df_merged["fecha_reserva"] = df_merged["fecha_reserva"].fillna("2025-02-22")
    return df_merged
 
def combinar_hoteles(mergeo_hoteles_propios, mergeo_competencia):
    """Concatena los DataFrames de hoteles propios y de competencia y renombra la columna de 'estrellas' a 'valoración'."""
    hoteles_mergeo = pd.concat([mergeo_hoteles_propios, mergeo_competencia], axis=0)
    hoteles_mergeo = hoteles_mergeo.rename(columns={"estrellas": "valoración"})
    return hoteles_mergeo
 
def generar_id_clientes(hoteles_mergeo):
    """Genera un ID único para cada cliente basado en su correo electrónico."""
    emails_unicos = hoteles_mergeo['mail'].unique()
    email_a_id = {email: f"cliente{i+1}" for i, email in enumerate(emails_unicos)}
    hoteles_mergeo['id_cliente'] = hoteles_mergeo['mail'].map(email_a_id)
    return hoteles_mergeo

def actualizar_ciudad(hoteles_mergeo):
    """Reemplaza valores nulos en 'ciudad' con 'Madrid' y asigna el ID de ciudad."""
    hoteles_mergeo['ciudad'] = 'Madrid'
    hoteles_mergeo['id_ciudad'] = 1
    hoteles_mergeo['id_ciudad'] = hoteles_mergeo['id_ciudad'].astype(int)
    return hoteles_mergeo

def convertir_tipos(df_eventos):
    """Convierte las columnas del DataFrame a los tipos adecuados."""
    df_eventos["código_postal"] = df_eventos["código_postal"].replace("No disponible", 0).astype(int)
    df_eventos["fecha_inicio"] = pd.to_datetime(df_eventos["fecha_inicio"], errors="coerce")
    df_eventos["fecha_fin"] = pd.to_datetime(df_eventos["fecha_fin"], errors="coerce")
    return df_eventos

def convertir_tipos(df_eventos):
    """Convierte las columnas del DataFrame a los tipos adecuados."""
    df_eventos["codigo_postal"] = df_eventos["código_postal"].replace("No disponible", 0).astype(int)
    df_eventos["fecha_inicio"] = pd.to_datetime(df_eventos["fecha_inicio"], errors="coerce")
    df_eventos["fecha_fin"] = pd.to_datetime(df_eventos["fecha_fin"], errors="coerce")
    df_eventos.rename(columns={'código_postal': 'codigo_postal', 'dirección': 'direccion', 'organización': 'organizacion'}, inplace=True)
    df_eventos["nombre_ciudad"] = "Madrid"
    return df_eventos

def limpiar_dataframe(df):
    """
    Limpia el DataFrame:
    - Extrae solo el nombre del hotel de la columna "nombre_hotel".
    - Extrae solo la puntuación de la columna "estrellas" y la convierte a un formato decimal.
    """
    
    # Limpiar "nombre_hotel"
    df["nombre_hotel"] = df["nombre_hotel"].str.split("\n").str[0]  # Separa por salto de línea y toma solo el primer valor
    
    # Limpiar "estrellas" y asegurarnos de que tenga formato decimal
    def limpiar_valoracion(valor):
        match = re.search(r"\d+(\.\d+)?", valor)  # Buscar número con o sin decimal
        if match:
            num = match.group(0)  # Extraer el número
            return f"{float(num):.1f}"  # Convertir a float con un decimal
        return "No disponible"  # Si no hay número, mantener como "No disponible"

    df["estrellas"] = df["estrellas"].apply(limpiar_valoracion)
    
    return df