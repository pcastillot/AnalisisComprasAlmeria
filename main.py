import streamlit as st
import pandas as pd
import numpy as np
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

# Hasta que no pulsemos el boton no intentará cargar los datos
if st.button('Cargar datos'):
    data_load_state = st.text('Cargando datos...')
    data = load_data(nFilas)
    data_load_state.text("Completado!")

    #if st.checkbox('Mostrar datos'):
    #    st.subheader('Datos')
    #    st.write(data)

    st.subheader('Numero de compras por franja horaria')
    # Obtenemos las compras que hay por cada franja horaria y creamos un DataFrame para mostrarlo en el gráfico
    counts = Counter(data['franja_horaria'])
    df = pd.DataFrame.from_dict(counts, orient='index')
    st.bar_chart(df)

    # Creamos un slider para filtrar los resultados en el mapa
    hour_to_filter = st.slider('Hora', 0, 23, 16, step=2)
    # Como nos devuelve un entero, lo hacemos string y cambiamos para que pueda compararse con los datos del DataFrame
    hour_to_filter = str(hour_to_filter) + '-' + str(hour_to_filter+2)
    filtered_data = data[data["franja_horaria"] == hour_to_filter]

    # Preparamos el titulo y cargamos los datos en el mapa
    st.subheader('Mapa de compras en el rango ' + hour_to_filter)
    st.map(filtered_data)