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
    df = pd.read_fwf(
        "clusters_report.txt",
        widths=[7, 16, 16, 79],
        header=0,
        skiprows=[1, 2, 3]
    )

    # Rellenar los valores nulos con los valores anteriores en las columnas de cluster, palabras clave y porcentaje
    df['Cluster'].fillna(method='ffill', inplace=True)
    df['Cantidad de'].fillna(method='ffill', inplace=True)
    df['Porcentaje de'].fillna(method='ffill', inplace=True)

    # Reemplazar los valores de porcentaje para quitar el signo % y convertirlos en números flotantes
    df['Porcentaje de'] = df['Porcentaje de'].str.replace(',', '.').str.rstrip('%').astype(float)

    # Agrupar por cluster, cantidad de palabras clave y porcentaje de palabras clave y unir las palabras clave
    df = df.groupby(['Cluster', 'Cantidad de', 'Porcentaje de'])['Principales palabras clave'].apply(lambda x: ' '.join(x)).reset_index()

    # Reemplazar los nombres de las columnas y realizar otras transformaciones necesarias
    df.columns = ['cluster', 'cantidad_de_palabras_clave', 'porcentaje_de_palabras_clave', 'principales_palabras_clave']
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace(r'\s{2,}', ' ', regex=True)
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace('.', '', regex=True)

    return df


