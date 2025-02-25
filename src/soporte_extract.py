import os
from dotenv import load_dotenv
import requests
import pandas as pd
from datetime import datetime

# Cargar variables de entorno
load_dotenv()

# Obtener la URL de la API desde el archivo .env
API_URL = os.getenv("API_URL")

def extraer_datos_api():
    """Extrae datos desde la API y devuelve un DataFrame con eventos filtrados."""
    if not API_URL:
        print("❌ Error: API_URL no está definida en el archivo .env")
        return None
    try:
        response = requests.get(API_URL)
        if response.status_code != 200:
            print("❌ Error al obtener los datos de la API")
            return None
        
        data = response.json()
        eventos = data.get("@graph", [])
        
        fecha_entrada = datetime.strptime("2025-03-01", "%Y-%m-%d").date()
        fecha_salida = datetime.strptime("2025-03-02", "%Y-%m-%d").date()
        
        eventos_fechas = {
            "nombre_evento": [], "url_evento": [], "codigo_postal": [],
            "direccion": [], "horario": [], "organizacion": [],
            "fecha_inicio": [], "fecha_fin": []
        }

        def extraer_nombre_desde_url(url):
            if url:
                partes = url.split("/")
                return partes[-1] if partes else "No disponible"
            return "No disponible"
        
        for evento in eventos:
            try:
                fecha_inicio_str = evento.get("dtstart", "")
                fecha_fin_str = evento.get("dtend", fecha_inicio_str)
                fecha_inicio = datetime.fromisoformat(fecha_inicio_str).date()
                fecha_fin = datetime.fromisoformat(fecha_fin_str).date()
                
                if fecha_inicio <= fecha_salida and fecha_fin >= fecha_entrada:
                    address = evento.get("address", {})
                    codigo_postal = address.get("area", {}).get("postal-code", "No disponible")
                    calle = address.get("area", {}).get("street-address", "")
                    barrio = extraer_nombre_desde_url(address.get("area", {}).get("@id", ""))
                    distrito = extraer_nombre_desde_url(address.get("district", {}).get("@id", ""))
                    direccion = ", ".join([parte for parte in [calle, barrio, distrito] if parte and parte != "No disponible"])
                    horario = evento.get("time", "No disponible")
                    
                    eventos_fechas["nombre_evento"].append(evento.get("title", "Sin título"))
                    eventos_fechas["url_evento"].append(evento.get("link", "Sin URL"))
                    eventos_fechas["codigo_postal"].append(codigo_postal)
                    eventos_fechas["direccion"].append(direccion if direccion else "No disponible")
                    eventos_fechas["horario"].append(horario)
                    eventos_fechas["organizacion"].append(evento.get("organization", {}).get("organization-name", "No disponible"))
                    eventos_fechas["fecha_inicio"].append(fecha_inicio)
                    eventos_fechas["fecha_fin"].append(fecha_fin)
            except ValueError:
                continue

        return pd.DataFrame(eventos_fechas)
    except Exception as e:
        print(f"❌ Error al conectar con la API: {e}")
        return None

 
  

  
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time

WEB_URL = os.getenv("WEB_URL")

def extraer_datos_web():
    """Realiza web scraping en la página web especificada en el .env y devuelve un DataFrame."""
    
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(WEB_URL)
    
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "hotelblock__content"))
        )
    except:
        print("❌ No se encontraron hoteles. Puede que la página tarde en cargar.")
        driver.quit()
        return None
    
    hoteles = driver.find_elements(By.CLASS_NAME, "hotelblock__content")
    datos_hoteles = []
    
    for index in range(len(hoteles)):
        try:
            hoteles = driver.find_elements(By.CLASS_NAME, "hotelblock__content")
            hotel = hoteles[index]  
            driver.execute_script("arguments[0].scrollIntoView();", hotel)
            time.sleep(1)
            
            nombre = hotel.find_element(By.CLASS_NAME, "title-block").text.strip()
            valoracion = hotel.find_element(By.CLASS_NAME, "hotelblock__content-ratings").text.strip()
            
            try:
                precio = hotel.find_element(By.CLASS_NAME, "rate-details__price1 .booking-price__number").text.strip()
            except:
                try:
                    precio = hotel.find_element(By.CLASS_NAME, "rate-details__price2 .booking-price__number").text.strip()
                except:
                    precio = "No disponible"
            
            datos_hoteles.append({
                "nombre_hotel": nombre,
                "estrellas": valoracion,
                "precio_noche": precio
            })
        except Exception as e:
            print(f"❌ Error al extraer datos del hotel {index+1}: {e}")
    
    driver.quit()
    return pd.DataFrame(datos_hoteles)