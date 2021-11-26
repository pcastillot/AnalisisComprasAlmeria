import streamlit as st
import pandas as pd
import numpy as np
import pydeck as pdk
from collections import Counter

# Titulo de la pagina
st.title('Compras en Almeria')

DATA_URL = "datos/cards_nuevo.csv"

# Mediante st.cache guardamos en cache los datos para no tener que cargarlos en cada prueba
# Funcion para cargar n filas del csv
@st.cache
def load_data(nrows):
    # DataFrame del csv a cargar
    data = pd.read_csv(DATA_URL, nrows=nrows, delimiter="|")
    # Pasamos las cabeceras a minuscula y le damos formate fecha a la fecha
    lowercase = lambda x: str(x).lower()
    data.rename(lowercase, axis='columns', inplace=True)
    data['dia'] = pd.to_datetime(data['dia'], yearfirst=True)
    print(data)
    return data


# COMIENZO
# Input para obtener las filas a cargar
nFilas = st.number_input("Número de filas a cargar: ", 0, 890612, step=1)

data = []

# Hasta que no pulsemos el boton no intentará cargar los datos
#if st.button('Cargar datos'):
data_load_state = st.text('Cargando datos...')
data = load_data(nFilas)
data_load_state.text("Completado!")

#if st.checkbox('Mostrar datos'):
#    st.subheader('Datos')
#    st.write(data)

st.subheader('Numero de compras por franja horaria')
# Obtenemos las compras que hay por cada franja horaria y creamos un DataFrame para mostrarlo en el gráfico
counts = Counter(data['franja_horaria'])
df_horas_compra = pd.DataFrame.from_dict(counts, orient='index')
st.bar_chart(df_horas_compra)

# Creamos un slider para filtrar los resultados en el mapa
hour_to_filter = st.slider('Hora', 0, 22, 16, step=2)
# Como nos devuelve un entero, lo hacemos string y cambiamos para que pueda compararse con los datos del DataFrame
hour_to_filter = str(hour_to_filter) + '-' + str(hour_to_filter+2)
filtered_data = data[data["franja_horaria"] == hour_to_filter]

# Preparamos el titulo y cargamos los datos en el mapa
st.subheader('Mapa de compras en el rango ' + hour_to_filter)

st.pydeck_chart(pdk.Deck(
    map_style='mapbox://styles/mapbox/light-v9',
    initial_view_state=pdk.ViewState(
    latitude=36.838139,
    longitude=-2.459740,
    zoom=8,
    pitch=50,
    ),
    layers=[
        pdk.Layer(
            "ArcLayer",
            data=filtered_data,
            get_source_position=["lon_cliente", "lat_cliente"],
            get_target_position=["lon", "lat"],
            get_source_color=[200, 30, 0, 160],
            get_target_color=[200, 30, 0, 160],
            auto_highlight=True,
            width_scale=0.0001,
            get_width="outbound",
            width_min_pixels=3,
            width_max_pixels=30,
            id="Movimientos de compra",
        ),
        pdk.Layer(
            "HexagonLayer",
            data=filtered_data,
            get_position=["lon_cliente", "lat_cliente"],
            radius=2000,
            elevation_scale=400,
            elevation_range=[0, 600],
            extruded=True,
            id="Compras por CP",
        ),
    ],
))