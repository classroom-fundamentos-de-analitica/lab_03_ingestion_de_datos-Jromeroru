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
    df = pd.read_fwf (
        "clusters_report.txt",
        widths=[7, 16, 16, 79],
        header=[0],
        skiprows=[1, 2, 3]
    )

    lastcl = 1
    lastpcl = 105
    lastp = "15,9 %"
    for i, _ in df.iterrows():
        if df.iloc[i,0] != lastcl and not pd.isna(df.iloc[i,0]):
            lastcl = df.iloc[i,0]
            lastpcl = df.iloc[i,1]
            lastp = df.iloc[i,2]
        else:
            df.iloc[i,0] = lastcl
            df.iloc[i,1] = lastpcl
            df.iloc[i,2] = lastp

    df = df.groupby(["Cluster", "Cantidad de", "Porcentaje de"])
    df = df.agg(lambda x: " ".join(x)).reset_index()
    df["Principales palabras clave"] = df["Principales palabras clave"].str.replace(r"\s{2,}", " ", regex=True)
    df["Principales palabras clave"] = df["Principales palabras clave"].str.replace(".", "", regex=True)
    df["Porcentaje de"] = df["Porcentaje de"].str.slice(0, -2)
    df["Porcentaje de"] = df["Porcentaje de"].str.replace(",", ".").astype(float)

    df.columns = [
            "cluster",
            "cantidad_de_palabras_clave",
            "porcentaje_de_palabras_clave",
            "principales_palabras_clave"
    ]

    return df

