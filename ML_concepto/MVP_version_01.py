import streamlit as st
import folium
from streamlit.components.v1 import html
import base64
import random
import math

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
    # Crear el mapa centrado en la ubicación seleccionada
    mapa = folium.Map(location=[lat, lon], zoom_start=15, control_scale=True)

    # Añadir un marcador con icono azul personalizado
    folium.Marker(
        [lat, lon],
        tooltip='Ubicación seleccionada',
        icon=folium.Icon(color='blue', icon='info-sign')
    ).add_to(mapa)

    # Obtener el HTML del mapa
    mapa_html = mapa._repr_html_()

    # Añadir el HTML del mapa a Streamlit con estilo CSS
    html_content = f"""
    <div style="width: 800px; height: 450px; overflow: hidden;">
        {mapa_html}
    </div>
    """

    # Renderizar el HTML en Streamlit
    html(html_content, height=450, width=800)


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
    options=['Seleccione una opción',
             'Restaurante', 'Hotel', 'Entretenimiento'],
    format_func=lambda x: 'Seleccione una opción' if x == 'Seleccione una opción' else x
)

# Apartado de ocasión
ocasion = st.selectbox(
    'Ocasión',
    options=['Seleccione una opción', 'Informal', 'Formal'],
    format_func=lambda x: 'Seleccione una opción' if x == 'Seleccione una opción' else x
)

# Apartado de presupuesto
presupuesto = st.selectbox(
    'Presupuesto',
    options=['Seleccione una opción', '0-100$',
             '101-200$', '201-500$', '501-1000$', '+1000$'],
    format_func=lambda x: 'Seleccione una opción' if x == 'Seleccione una opción' else x
)

# Apartado de radio de búsqueda
radio_busqueda = st.selectbox(
    'Radio de búsqueda (KM) - Estadio',
    options=['Seleccione una opción', '0-1 km', '1-5 km', '5-10 km'],
    format_func=lambda x: 'Seleccione una opción' if x == 'Seleccione una opción' else x
)

# Función para generar una ubicación aleatoria dentro de un radio


def generar_coordenadas_aleatorias(lat, lon, radio_km):
    # Conversión de distancia a grados
    delta_lat = radio_km / 111  # Convertimos km a grados de latitud

    # Un grado de longitud varía según la latitud
    delta_lon = radio_km / (111 * math.cos(math.radians(lat)))

    # Generar un desplazamiento aleatorio dentro del círculo
    lat_offset = random.uniform(-delta_lat, delta_lat)
    lon_offset = random.uniform(-delta_lon, delta_lon)

    # Nuevas coordenadas dentro del radio
    nueva_lat = lat + lat_offset
    nueva_lon = lon + lon_offset

    return nueva_lat, nueva_lon


# Mapa de ubicaciones relacionadas con la ciudad seleccionada
if ciudad_sede != 'Seleccione una opción':
    buscar = st.button('Buscar')

    if buscar:
        st.session_state.buscar = True

if st.session_state.buscar:
    st.write(
        f"Mostrando los mejores resultados para su búsqueda en {ciudad_sede}...")

    # Asignar coordenadas en base a la ciudad sede seleccionada (coordenadas de los estadios)
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

    # Ajustar las coordenadas según el radio de búsqueda
    if radio_busqueda == '0-1 km':
        radio = 1  # en kilómetros
    elif radio_busqueda == '1-5 km':
        radio = 5  # en kilómetros
    elif radio_busqueda == '5-10 km':
        radio = 10  # en kilómetros
    else:
        radio = 0  # Sin desplazamiento

    # Generar una ubicación aleatoria dentro del radio seleccionado
    nueva_lat, nueva_lon = generar_coordenadas_aleatorias(lat, lon, radio)

    # Ejemplos de prueba
    # Mostrar resultados según la necesidad seleccionada
    if necesidad_cliente == 'Restaurante' or necesidad_cliente == 'Seleccione una opción':
        st.write("### Resultados para Restaurantes")
        st.write(" **Nombre del establecimiento:** La Casa de los Tacos")
        st.write(" **Calificación:** 4.7")
        st.write(
            " **Descripción:** Restaurante mexicano con auténticos tacos al pastor y margaritas.")
        st.write(" **Ubicación:**")
        generar_mapa(nueva_lat, nueva_lon)  # Muestra el mapa interactivo

    if necesidad_cliente == 'Hotel' or necesidad_cliente == 'Seleccione una opción':
        st.write("### Resultados para Hoteles")
        st.write(" **Nombre del establecimiento:** Hotel Central")
        st.write(" **Calificación:** 4.5")
        st.write(
            " **Descripción:** Hotel con comodidades modernas y desayuno gratuito.")
        st.write(" **Ubicación:**")
        generar_mapa(nueva_lat, nueva_lon)  # Muestra el mapa interactivo

    if necesidad_cliente == 'Entretenimiento' or necesidad_cliente == 'Seleccione una opción':
        st.write("### Resultados para Entretenimiento")
        st.write(" **Nombre del establecimiento:** Parque de diversiones Max")
        st.write(" **Calificación:** 4.4")
        st.write(" **Descripción:** Múltiples áreas para disfrutar con la familia.")
        st.write(" **Ubicación:**")
        generar_mapa(nueva_lat, nueva_lon)  # Muestra el mapa interactivo
