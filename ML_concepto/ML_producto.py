import streamlit as st
import folium
from streamlit.components.v1 import html
import mysql.connector
import os
import base64
import math
from dotenv import load_dotenv

# Conectar la base de datos
load_dotenv()
password_bd = os.getenv("PASS_BD")
user_bd = os.getenv("USER_BD")
host_name = os.getenv("HOST_NAME")
name_bd = os.getenv("NAME_BD")


def BD_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_bd,
            passwd=password_bd,
            database=name_bd,
            port=3306
        )
        print("MYSQL DATABASE connection successful")
    except mysql.connector.Error as err:
        print(f"Error: '{err}'")
    return connection


conn = BD_connection()

# Función para añadir imagen de fondo


def add_bg_image(image_file):
    with open(image_file, "rb") as f:
        encoded_image = base64.b64encode(f.read()).decode()
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-image: url(data:image/jpeg;base64,{encoded_image});
            background-size: cover;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )


# Añadir imagen de fondo
add_bg_image("fondo.jpg")

# Título de la aplicación
st.markdown("<h1 style='text-align: center; font-size: 50px;'>QuantumCore Analytics</h1>",
            unsafe_allow_html=True)
st.markdown("<h2 style='text-align: center; font-size: 30px;'>Sistema de recomendación para visitantes del Mundial FIFA 2026</h2>", unsafe_allow_html=True)

# Crear un espacio debajo del título
st.markdown("<br><br>", unsafe_allow_html=True)

# Función para generar el mapa interactivo usando folium con un marcador personalizado


def generar_mapa(lat, lon):
    mapa = folium.Map(location=[lat, lon], zoom_start=15, control_scale=True)
    folium.Marker(
        [lat, lon],
        tooltip='Ubicación seleccionada',
        icon=folium.Icon(color='blue', icon='info-sign')
    ).add_to(mapa)
    mapa_html = mapa._repr_html_()
    html_content = f"""
    <div style="width: 800px; height: 450px; overflow: hidden;">
        {mapa_html}
    </div>
    """
    html(html_content, height=450, width=800)

# Función para calcular la distancia entre dos puntos geográficos usando la fórmula de Haversine


def calcular_distancia(lat1, lon1, lat2, lon2):
    R = 6371  # Radio de la Tierra en kilómetros
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * \
        math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distancia = R * c
    return distancia


# Inicialización de st.session_state para evitar que los resultados desaparezcan
if "buscar" not in st.session_state:
    st.session_state.buscar = False

# Apartado de selección de ciudad sede (estadio)
ciudad_sede = st.selectbox(
    'Ciudad sede',
    options=['Seleccione una opción', 'Atlanta', 'Boston', 'Dallas', 'Houston', 'Kansas City',
             'Los Ángeles', 'Miami', 'Nueva York/Nueva Jersey', 'Filadelfia',
             'San Francisco Bay Area', 'Seattle'],
    format_func=lambda x: 'Seleccione una opción' if x == 'Seleccione una opción' else x
)

# Apartado de necesidad del cliente
necesidad_cliente = st.selectbox(
    'Necesidad del cliente',
    options=['Seleccione una opción', 'Restaurante', 'Hotel'],
    format_func=lambda x: 'Seleccione una opción' if x == 'Seleccione una opción' else x
)

# Apartado de presupuesto (con alias)
presupuesto = st.selectbox(
    'Presupuesto',
    options=['Seleccione una opción',
             '$ (0-9)', '$$ (10-99)', '$$$ (100-999)', '$$$$ (+1000)'],
    format_func=lambda x: 'Seleccione una opción' if x == 'Seleccione una opción' else x
)

# Apartado de radio de búsqueda
radio_busqueda = st.selectbox(
    'Radio de búsqueda (KM) - Estadio',
    options=['Seleccione una opción', '0-1 km', '1-5 km', '5-10 km'],
    format_func=lambda x: 'Seleccione una opción' if x == 'Seleccione una opción' else x
)

# Función para buscar resultados desde la base de datos con radio y top 5 calificaciones


def buscar_resultados(conn, lat_sede, lon_sede, necesidad_cliente, presupuesto, radio):
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT name, description, avg_rating, price, latitude, longitude,
        ( 6371 * acos( cos( radians(%s) ) * cos( radians( latitude ) ) 
        * cos( radians( longitude ) - radians(%s) ) + sin( radians(%s) ) 
        * sin( radians( latitude ) ) ) ) AS distance
        FROM sites
        WHERE category = %s AND price = %s
        HAVING distance <= %s
        ORDER BY avg_rating DESC
        LIMIT 5
    """

    precio_alias = {
        '$ (0-9)': '$',
        '$$ (10-99)': '$$',
        '$$$ (100-999)': '$$$',
        '$$$$ (+1000)': '$$$$'
    }

    params = (lat_sede, lon_sede, lat_sede, necesidad_cliente,
              precio_alias[presupuesto], radio)
    cursor.execute(query, params)
    return cursor.fetchall()


# Mapa de ubicaciones relacionadas con la ciudad seleccionada
if ciudad_sede != 'Seleccione una opción':
    buscar = st.button('Buscar')

    if buscar:
        st.session_state.buscar = True

if st.session_state.buscar:
    st.write(
        f"Mostrando los mejores resultados para su búsqueda en {ciudad_sede}...")

    coordenadas_ciudad = {
        'Atlanta': (33.755489, -84.401993),
        'Boston': (42.0909458, -71.264346),
        'Dallas': (32.747841, -97.093628),
        'Houston': (29.684860, -95.411667),
        'Kansas City': (39.048786, -94.484566),
        'Los Ángeles': (33.953587, -118.339630),
        'Miami': (25.957960, -80.239311),
        'Nueva York/Nueva Jersey': (40.813778, -74.074310),
        'Filadelfia': (39.900898, -75.168098),
        'San Francisco Bay Area': (37.4020148919, -121.968869091),
        'Seattle': (47.59509, -122.332245)
    }

    lat, lon = coordenadas_ciudad[ciudad_sede]

    if radio_busqueda == '0-1 km':
        radio = 1
    elif radio_busqueda == '1-5 km':
        radio = 5
    elif radio_busqueda == '5-10 km':
        radio = 10
    else:
        radio = 0

    resultados = buscar_resultados(
        conn, lat, lon, necesidad_cliente, presupuesto, radio)

    if resultados:
        for resultado in resultados:
            st.write(f"### Nombre: {resultado['name']}")
            st.write(f"**Calificación:** {resultado['avg_rating']}")
            st.write(f"**Descripción:** {resultado['description']}")
            generar_mapa(resultado['latitude'], resultado['longitude'])
    else:
        st.write("No se encontraron resultados para su búsqueda.")
