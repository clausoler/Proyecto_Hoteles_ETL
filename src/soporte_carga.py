import psycopg2

def conectar_bd():
    """Establece conexión con la base de datos PostgreSQL."""
    conn = psycopg2.connect(
        dbname="hoteles_eventos_madrid",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )
    return conn

def verificar_conexion(conn):
    """Verifica la conexión a la base de datos ejecutando una consulta simple."""
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    print(cursor.fetchone())
    cursor.close()

def insertar_ciudades(conn, df):
    """Inserta datos en la tabla ciudades."""
    cursor = conn.cursor()
    for _, fila in df.iterrows():
        consulta = "INSERT INTO ciudades (id_ciudad, nombre_ciudad) VALUES (%s, %s)"
        cursor.execute(consulta, (fila['id_ciudad'], fila['nombre_ciudad']))
    conn.commit()
    cursor.close()

def insertar_clientes(conn, df):
    """Inserta datos en la tabla clientes."""
    cursor = conn.cursor()
    for _, fila in df.iterrows():
        consulta = "INSERT INTO clientes (id_cliente, nombre, apellido, email) VALUES (%s, %s, %s, %s)"
        cursor.execute(consulta, (fila['id_cliente'], fila['nombre'], fila['apellido'], fila['mail']))
    conn.commit()
    cursor.close()

def insertar_reservas(conn, df):
    """Inserta datos en la tabla reservas."""
    cursor = conn.cursor()
    for _, fila in df.iterrows():
        consulta = "INSERT INTO reservas (id_reserva, fecha_reserva, inicio_estancia, final_estancia, precio_noche, id_hotel, id_cliente ) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(consulta, (fila['id_reserva'], fila['fecha_reserva'], fila['inicio_estancia'], fila['final_estancia'], fila['precio_noche'], fila['id_hotel'], fila['id_cliente']))
    conn.commit()
    cursor.close()

def insertar_hoteles(conn, df):
    """Inserta datos en la tabla hoteles."""
    cursor = conn.cursor()
    for _, fila in df.iterrows():
        consulta = "INSERT INTO hoteles (nombre_hotel, competencia, valoracion, id_ciudad) VALUES (%s, %s, %s, %s)"
        cursor.execute(consulta, (fila['nombre_hotel'], fila['competencia'], fila['valoracion'], fila['id_ciudad']))
    conn.commit()
    cursor.close()

def insertar_eventos(conn, df):
    """Inserta datos en la tabla eventos."""
    cursor = conn.cursor()
    for _, fila in df.iterrows():
        consulta = "INSERT INTO eventos (nombre_evento, url_evento, codigo_postal, direccion, horario, fecha_inicio, fecha_fin, organizacion, id_ciudad) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(consulta, (fila['nombre_evento'], fila['url_evento'], fila['codigo_postal'], fila['direccion'], fila['horario'], fila['fecha_inicio'], fila['fecha_fin'], fila['organzacion'], fila['id_ciudad']))
    conn.commit()
    cursor.close()

def cerrar_conexion(conn):
    """Cierra la conexión con la base de datos."""
    conn.close()
 
