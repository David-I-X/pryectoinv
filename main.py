from fastapi import FastAPI, Body, Request, HTTPException
from pydantic import BaseModel

# Importaciones de librerías para análisis de datos
import pandas as pd
from collections import Counter

#Modelo de datos:

#Creamos un modelo de datos para las peticiones a la API:
class Peticion(BaseModel):
    genero: str
    año: int

#Instancia de FastAPI:
app = FastAPI()


#Funciones para los Endpoints:

#1. PlayTimeGenre:
@app.get("/playtimegenre/{genero}")
async def PlayTimeGenre(request: Request, genero: str):
    """
    Obtiene el año de lanzamiento con más horas jugadas para un género específico.

    Parámetros:
        genero: El género de los juegos a analizar.

    Respuesta:
        Un diccionario con el año de lanzamiento con más horas jugadas para el género especificado.
    """

   # Carga de datos
    try:
     df_juegos = pd.read_parquet("ETL/games.parquet", columns=["item_id", "tags", "release_date"])
     df_items = pd.read_parquet("ETL/items.parquet", columns=["item_id", "playtime_forever"])
    except FileNotFoundError:
     raise Exception("Error al cargar los datos")

  # Filtrado por género
    df_genero = df_juegos.loc[df_juegos["tags"].str.contains(genero, case=False)]
    df_genero = df_genero.merge(df_items[['item_id', 'playtime_forever']], on='item_id', how='left')

  # Año con más horas jugadas
    if "release_date" in df_genero.columns:
       anio_lanzamiento = pd.to_datetime(df_genero['release_date']).dt.year
       df_horas = df_genero.groupby(anio_lanzamiento)["playtime_forever"].agg("sum")
       año_max = df_horas.idxmax()
       return {"Año de lanzamiento con más horas jugadas para Género " + genero: año_max}
    else:
      return {"Mensaje": "No se pudo obtener el año de lanzamiento debido a problemas con la columna `release_date`"}



#2. UserForGenre:
@app.get("/user_for_genre/{genero}")
async def user_for_genre(request: Request, genero: str):
    """
    Obtiene el usuario con más horas jugadas para un género específico.

    Parámetros:
        genero: El género del juego (por ejemplo, "Acción").

    Respuesta:
        Un diccionario con el usuario con más horas jugadas y un desglose de las horas jugadas por usuario.
    """

    # Carga de datos
    df_reviews = pd.read_parquet("ETL/reviews.parquet", columns=['user_id', 'item_id'])
    df_juegos = pd.read_parquet("ETL/games.parquet", columns=['item_id', 'tags'])

    # Filtrado por género (buscar en la columna 'tags' del DataFrame juegos)
    try:
        df_genero = df_juegos[df_juegos["tags"].str.contains(genero, case=False)]
        df_genero = df_genero[df_genero["item_id"].isin(df_juegos[df_juegos["tags"].str.contains(genero, case=False)]['item_id'])]
    except KeyError:
        raise HTTPException(status_code=404, detail="Género no encontrado")

    # Agrupación por usuario y cálculo de horas jugadas (asumiendo playtime_forever en df_items)
    df_items = pd.read_parquet("ETL/items.parquet", columns=['user_id', 'item_id', 'playtime_forever'])
    df_items['playtime_forever'] = df_items['playtime_forever'].astype('float32')
    df_horas = df_genero.merge(df_items, on='item_id', how='left').groupby('user_id')['playtime_forever'].sum()

    # Usuario con más horas jugadas
    usuario_max = df_horas.idxmax()

    return {"Usuario con más horas jugadas para Género " + genero: usuario_max, "Horas jugadas": df_horas.to_dict()}
