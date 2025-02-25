import pandas as pd
from src.soporte_carga import conectar_bd, verificar_conexion, insertar_ciudades, insertar_clientes, insertar_reservas, insertar_hoteles, insertar_eventos, cerrar_conexion

# Cargar los archivos de datos limpios
print("📥 Cargando datos...")

df_ciudades = pd.read_pickle("..data/data_transform/reservas_hoteles_limpio.pkl")
df_clientes = pd.read_pickle("..data/data_transform/reservas_hoteles_limpio.pkl")
df_reservas = pd.read_pickle("..data/data_transform/reservas_hoteles_limpio.pkl")
df_hoteles = pd.read_pickle("..data/data_transform/reservas_hoteles_limpio.pkl")
df_eventos = pd.read_pickle("../data/data_raw/eventos_madrid.pkl")

# Conectar a la base de datos
print("🔗 Conectando a la base de datos...")
conn = conectar_bd()
verificar_conexion(conn)

# Insertar datos en la base de datos
print("📌 Insertando datos en la tabla ciudades...")
insertar_ciudades(conn, df_ciudades)
print("📌 Insertando datos en la tabla clientes...")
insertar_clientes(conn, df_clientes)
print("📌 Insertando datos en la tabla reservas...")
insertar_reservas(conn, df_reservas)
print("📌 Insertando datos en la tabla hoteles...")
insertar_hoteles(conn, df_hoteles)
print("📌 Insertando datos en la tabla eventos...")
insertar_eventos(conn, df_eventos)

# Cerrar la conexión
print("✅ Datos insertados correctamente. Cerrando conexión...")
cerrar_conexion(conn)

print("🚀 Proceso de carga completado exitosamente!")
