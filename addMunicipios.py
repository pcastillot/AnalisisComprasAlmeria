import csv
import pandas as pd

# Abrimos el nuevo fichero en el que se escribiran los datos
with open("datos/cards_nuevo.csv", "w", encoding="UTF-8", newline="") as fileBueno:
    datosWrite = csv.writer(fileBueno, delimiter="|")

    # Abrimos el fichero del que leeremos los datos
    with open("datos/cards.csv", "r", encoding="UTF-8") as file:
        datosRead = csv.reader(file, delimiter="|")

        # Creamos un DataFrame con el listado de CPs e inicializamos el contador
        dfCP = pd.read_csv("datos/listado-codigos-postales-con-LatyLon.csv", delimiter=";")
        i = 0

        # Recorremos todas las filas del csv que queremos leer
        for row in datosRead:
            # Si nos encontramos en la cabecera, la escribimos
            if i == 0:
                datosWrite.writerow([row[0], "POB_CLIENTE", "LAT_CLIENTE", "LON_CLIENTE", row[1], "POB_COMERCIO",
                                     "LAT_COMERCIO", "LON_COMERCIO", row[2], row[3], row[4], row[5], row[6]])

            else:
                # Sacamos los codigos postales del csv que estamos leyendo
                cpCliente = int(row[0])
                cpComercio = int(row[1])

                # Buscamos la lat, lon y poblacion en la que se encuentra cada CP, comparando en el DataFrame de los CPs
                latCliente = dfCP.loc[dfCP['codigopostalid'] == cpCliente]['lat'].values[0]
                lonCliente = dfCP.loc[dfCP['codigopostalid'] == cpCliente]['lon'].values[0]
                pobCliente = dfCP.loc[dfCP['codigopostalid'] == cpCliente]['poblacion'].values[0]
                latComercio = dfCP.loc[dfCP['codigopostalid'] == cpComercio]['lat'].values[0]
                lonComercio = dfCP.loc[dfCP['codigopostalid'] == cpComercio]['lon'].values[0]
                pobComercio = dfCP.loc[dfCP['codigopostalid'] == cpComercio]['poblacion'].values[0]

                # Escribimos la fila con los datos nuevos y antiguos en el archivo nuevo
                datosWrite.writerow([row[0], str(pobCliente), str(latCliente), str(lonCliente), str(row[1]),
                                     str(pobComercio), str(latComercio), str(lonComercio), row[2], row[3],
                                     row[4], row[5], row[6]])

                # Informacion sobre el progreso
                porcentaje = "{:.2f}".format((i*100)/890610)
                print(str(porcentaje) + "%  (" + str(i) + "/890610)")

            i += 1

