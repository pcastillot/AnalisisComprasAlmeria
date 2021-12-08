import streamlit as st
import pandas as pd
import pydeck as pdk
from collections import Counter
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, avg
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import StringType, FloatType

st.title('Compras en Almeria')

spark = SparkSession.builder.appName("DataFrame").getOrCreate()

DATA_CARDS = "datos/cards_nuevo.csv"
DATA_WEATHER = "datos/weather.csv"
RANGO_HORAS = ['00-02','02-04','04-06','06-08','08-10','10-12','12-14','14-16','16-18','18-20','20-22','22-24']
DICT_MES = {
    'Enero': '2015-01',
    'Febrero': '2015-02',
    'Marzo': '2015-03',
    'Abril': '2015-04',
    'Mayo': '2015-05',
    'Junio': '2015-06',
    'Julio': '2015-07',
    'Agosto': '2015-08',
    'Septiembre': '2015-09',
    'Octubre': '2015-10',
    'Noviembre': '2015-11',
    'Diciembre': '2015-12'
}

# Funcion para cargar n filas del csv
def load_data(nFilas):

    # DataFrame del csv Cards
    dataCards = spark.read.options(delimiter="|", header=True, encoding='UTF-8').csv(DATA_CARDS).limit(nFilas)
    dataCards = dataCards.withColumn('importe', regexp_replace('importe', ',', '.'))  # data.mapInPandas(comas_por_puntos, data.schema)
    dataCards = dataCards.withColumn('importe', col('importe').cast(FloatType()))
    dataCards = dataCards.withColumn('lat', col('lat').cast(FloatType()))
    dataCards = dataCards.withColumn('lon', col('lon').cast(FloatType()))
    dataCards = dataCards.withColumn('lat_cliente', col('lat_cliente').cast(FloatType()))
    dataCards = dataCards.withColumn('lon_cliente', col('lon_cliente').cast(FloatType()))
    dataCards = dataCards.withColumn('dia', col('dia').cast(StringType()))

    # DataFrame del csv Weather
    dataWeather = spark.read.options(delimiter=";", header=True, encoding='UTF-8').csv(DATA_WEATHER)
    dataWeather = dataWeather.withColumn('TMax', col('TMax').cast(FloatType()))
    dataWeather = dataWeather.withColumn('HTMax', col('HTMax').cast(FloatType()))
    dataWeather = dataWeather.withColumn('TMin', col('TMin').cast(FloatType()))
    dataWeather = dataWeather.withColumn('TMed', col('TMed').cast(FloatType()))
    dataWeather = dataWeather.withColumn('HumMax', col('HumMax').cast(FloatType()))
    dataWeather = dataWeather.withColumn('HumMin', col('HumMin').cast(FloatType()))
    dataWeather = dataWeather.withColumn('HumMed', col('HumMed').cast(FloatType()))
    dataWeather = dataWeather.withColumn('VelViento', col('VelViento').cast(FloatType()))
    dataWeather = dataWeather.withColumn('DirViento', col('DirViento').cast(FloatType()))
    dataWeather = dataWeather.withColumn('Rad', col('Rad').cast(FloatType()))
    dataWeather = dataWeather.withColumn('Precip', col('Precip').cast(FloatType()))
    dataWeather = dataWeather.withColumn('ETo', col('ETo').cast(FloatType()))

    return dataCards, dataWeather



# COMIENZO
# Input para obtener las filas a cargar
nFilas = st.number_input("Número de filas a cargar: ", 0, 890612, step=1)
dataCards = []
if nFilas:
    data_load_state = st.text('Cargando datos...')
    dataCards, dataWeather = load_data(nFilas)

    data_load_state.text("Completado!")

    if st.checkbox('Mostrar datos'):
        st.subheader('Datos')
        st.write(dataCards.toPandas())

    st.subheader('Numero de compras por franja horaria')
    
    # Obtenemos las compras que hay por cada franja horaria y creamos un DataFrame para mostrarlo en el gráfico
    list_franja = []
    for col in dataCards.collect():
        list_franja.append(col["franja_horaria"])

    counts = Counter(list_franja)
    df_horas_compra = pd.DataFrame.from_dict(counts, orient='index')
    st.bar_chart(df_horas_compra)

    # Preparamos el titulo y cargamos los datos en el mapa
    st.subheader('Mapa de compras')

    # Creamos un slider para filtrar los resultados en el mapa
    hour_to_filter = st.select_slider('Hora', options=RANGO_HORAS)

    #Importe maximo del dataset
    importeMaxData = int(dataCards.agg({'importe': 'max'}).collect()[0]['max(importe)'])

    importe_to_filter = [None, None]
    if st.checkbox('Utilizar slider'):
        
        # Creamos un slider para filtrar por importe en el mapa
        importe_to_filter = st.slider('Importe', 0, importeMaxData, (0, 50), step=1)

    else:

        #Inputs importe
        importe_to_filter[0] = int(st.number_input("Importe mínimo: ", 0, importeMaxData, step=1))
        if importe_to_filter[0]:
            importe_to_filter[1] = int(st.number_input("Importe máximo: ", importe_to_filter[0], importeMaxData, step=1))

    if importe_to_filter[1]:
        filtered_data = dataCards.select('lat', 'lon', 'lat_cliente', 'lon_cliente', 'franja_horaria')\
            .filter(
                (dataCards.franja_horaria == hour_to_filter) &
                (dataCards.importe > importe_to_filter[0]) &
                (dataCards.importe < importe_to_filter[1])
            ).toPandas()

        st.write('Rango de horas:   ', hour_to_filter)
        st.write('Rango de importe: ', importe_to_filter[0], '-', importe_to_filter[1])
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
                ),
                pdk.Layer(
                    "HexagonLayer",
                    data=filtered_data,
                    get_position=["lon_cliente", "lat_cliente"],
                    radius=2000,
                    elevation_scale=400,
                    elevation_range=[0, 600],
                    extruded=True,
                ),
            ],
        ))

    # KPI compras por mes representado en barras
    st.subheader("Estadisticas por mes")
    df_mes = dataCards
    df_mes = df_mes.withColumn('dia', substring('dia', 1, 7))
    list_mes = []
    for col in df_mes.collect():
        list_mes.append(col["dia"])
    counts_mes = Counter(list_mes)
    df_mes_compra = pd.DataFrame.from_dict(counts_mes, orient='index')
    st.caption('Numero de compras por mes.')
    df_mes_compra.columns = ['nCompras']
    st.bar_chart(df_mes_compra)

    # Weather por mes
    weather_mes = dataWeather
    weather_mes = weather_mes.withColumn('FECHA', substring('FECHA', 1, 7))
    weather_mes = weather_mes.groupby('FECHA').agg(avg('TMax'), avg('TMed'), avg('TMin'))
    weather_mes = weather_mes.toPandas()
    weather_mes = weather_mes.set_index('FECHA')
    st.caption('Temperatura por mes.')
    st.line_chart(weather_mes)

    # KPI dinero gastado al mes
    df_gastos_mes = df_mes.select('dia', 'importe')
    df_gastos_mes = df_gastos_mes.groupby('dia').sum('importe')
    st.caption('Importe gastado por mes.')
    df_gastos_mes = df_gastos_mes.toPandas()
    df_gastos_mes = df_gastos_mes.set_index('dia')
    st.area_chart(df_gastos_mes)

    # Compras por dias del mes
    st.subheader('Estadísticas por dias del mes')
    mes = st.select_slider('Selecciona el mes', options=DICT_MES.keys(), value='Enero')

    # Numero de compras por dia
    df_dias_mes = dataCards.select('dia')
    df_dias_mes = df_dias_mes.filter(dataCards.dia.startswith(DICT_MES[mes]))
    list_dias_mes = [row[0] for row in df_dias_mes.select('dia').collect()]
    counts_dias = Counter(list_dias_mes)
    df_compras_dias_mes = pd.DataFrame.from_dict(counts_dias, orient='index')
    df_compras_dias_mes.columns = ['nCompras']
    st.caption('Numero de compras por dia del mes.')
    st.area_chart(df_compras_dias_mes)

    # Temperatura por dia
    df_temp_dia = dataWeather.select('FECHA', 'TMax', 'TMed', 'TMin')
    df_temp_dia = df_temp_dia.filter(df_temp_dia.FECHA.startswith(DICT_MES[mes]))
    df_temp_dia = df_temp_dia.toPandas().set_index('FECHA')
    st.caption('Temperatura por dia del mes.')
    st.line_chart(df_temp_dia)

    # Precipitaciones por dia
    df_prec_dia = dataWeather.select('FECHA', 'Precip')
    df_prec_dia = df_prec_dia.filter(df_prec_dia.FECHA.startswith(DICT_MES[mes]))
    df_prec_dia = df_prec_dia.toPandas().set_index('FECHA')
    st.caption('Precipitaciones por dia del mes.')
    st.line_chart(df_prec_dia)





