"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():
    # Leer archivo
    with open("clusters_report.txt", "r") as file:
        lines = file.readlines()
    # limpiar las lineas y remplazar multiples espacios por uno
    dataRaw = [" ".join(line.strip().split()) for line in lines]
    dataSinVacios = list(filter(None, dataRaw))
    # eliminar separador
    dataSinVacios.pop(2)

    # separar las dos primeras lineas que seran los nombres de las columnas
    headers = [dataSinVacios.pop(0), dataSinVacios.pop(0)]
    headers = generateHeaders(headers[0], headers[1])

    # agregar punto faltante para poder identificar cada fila
    dataSinVacios[23] = dataSinVacios[23] + "."
    # separar filas por el punto
    dataFilas = " ".join(dataSinVacios).split(".")
    # eliminar vacio final
    dataFilas.pop(-1)

    dataCleaned = []

    for fila in dataFilas:
        fila = fila.split("%")
        datosPalabrasClave = fila[0].split()
        palabrasClave = fila[1].strip()

        for i in range(3):
            datosPalabrasClave[i] = float(datosPalabrasClave[i].replace(",", "."))
            if i in [0, 1]:
                datosPalabrasClave[i] = int(datosPalabrasClave[i])

        datosPalabrasClave.append(palabrasClave)

        dataCleaned.append(datosPalabrasClave)

    df = pd.DataFrame(dataCleaned, columns=headers)
    
    return df

