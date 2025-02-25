import pandas as pd
import numpy as np
import requests
import os
from dotenv import load_dotenv
from datetime import datetime
from src.soporte_extract import extraer_datos_api, extraer_datos_web

# Extraer datos desde la API
print("📡 Extrayendo datos desde la API...")
df_eventos = extraer_datos_api()
if df_eventos is not None:
    print("✅ Datos de la API extraídos correctamente.")
    df_eventos.to_pickle("../data/data_raw/eventos_madrid.pkl")
    print("💾 Datos de eventos guardados en 'eventos_madrid.pkl'")
else:
    print("❌ Error en la extracción de datos de la API.")

# Extraer datos desde Web Scraping
print("🌐 Extrayendo datos desde la web...")
df_hoteles = extraer_datos_web()
if df_hoteles is not None:
    print("✅ Datos de hoteles extraídos correctamente.")
    df_hoteles.to_csv("../data/data_raw/hoteles_competencia.csv", index=False, encoding="utf-8")
    print("💾 Datos de hoteles guardados en 'hoteles_competencia.csv'")
else:
    print("❌ Error en la extracción de datos web.")

print("🚀 Extracción de datos completada.")