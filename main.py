from fastapi import FastAPI, Body, Request, HTTPException
from pydantic import BaseModel

# Importaciones de librerías para análisis de datos
import pandas as pd
from collections import Counter

#Modelo de datos:

app = FastAPI()

# Cargar tu DataFrame df_movies aquí
df_movies = pd.read_parquet('ETL\\movies.parquet')

@app.get("/cantidad_filmaciones_mes/{mes}")
def cantidad_filmaciones_mes(mes: str):
    meses = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
        "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
        "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
    }
    mes_numero = meses.get(mes.lower())
    if mes_numero is None:
        return "Mes inválido"
    
    df_movies['release_date'] = pd.to_datetime(df_movies['release_date'])
    cantidad = df_movies[df_movies['release_date'].dt.month == mes_numero].shape[0]
    return f"{cantidad} películas fueron estrenadas en el mes de {mes}"

@app.get("/cantidad_filmaciones_dia/{dia}")
def cantidad_filmaciones_dia(dia: str):
    dias = {
        "lunes": 0, "martes": 1, "miércoles": 2, "jueves": 3,
        "viernes": 4, "sábado": 5, "domingo": 6
    }
    dia_numero = dias.get(dia.lower())
    if dia_numero is None:
        return "Día inválido"
    
    df_movies['release_date'] = pd.to_datetime(df_movies['release_date'])
    cantidad = df_movies[df_movies['release_date'].dt.weekday == dia_numero].shape[0]
    return f"{cantidad} películas fueron estrenadas en los días {dia}"

@app.get("/score_titulo/{titulo}")
def score_titulo(titulo: str):
    film = df_movies[df_movies['title'].str.lower() == titulo.lower()]
    if film.empty:
        return "Película no encontrada"
    
    film_info = film.iloc[0]
    return f"La película {film_info['title']} fue estrenada en el año {film_info['release_year']} con un score/popularidad de {film_info['popularity']}"

@app.get("/votos_titulo/{titulo}")
def votos_titulo(titulo: str):
    film = df_movies[df_movies['title'].str.lower() == titulo.lower()]
    if film.empty:
        return "Película no encontrada"
    
    film_info = film.iloc[0]
    if film_info['vote_count'] < 2000:
        return "La película no cumple con el mínimo de 2000 valoraciones"
    
    promedio_votos = film_info['vote_average']
    return f"La película {film_info['title']} fue estrenada en el año {film_info['release_year']}. La misma cuenta con un total de {film_info['vote_count']} valoraciones, con un promedio de {promedio_votos}"


@app.get("/get_actor/{nombre_actor}")
def get_actor(nombre_actor: str):
    actor_films = df_movies[df_movies['cast'].apply(lambda x: nombre_actor.lower() in x.lower())]
    if actor_films.empty:
        return "Actor no encontrado"
    
    cantidad = actor_films.shape[0]
    total_retorno = actor_films['return'].sum()
    promedio_retorno = total_retorno / cantidad
    return f"El actor {nombre_actor} ha participado de {cantidad} cantidad de filmaciones, el mismo ha conseguido un retorno de {total_retorno} con un promedio de {promedio_retorno} por filmación"


@app.get("/get_director/{nombre_director}")
def get_director(nombre_director: str):
    director_films = df_movies[df_movies['director'].str.lower() == nombre_director.lower()]
    if director_films.empty:
        return "Director no encontrado"
    
    films_info = []
    for _, film in director_films.iterrows():
        ganancia = film['revenue'] - film['budget']
        films_info.append({
            "title": film['title'],
            "release_date": film['release_date'],
            "individual_return": film['return'],
            "budget": film['budget'],
            "ganancia": ganancia
        })
    
    total_retorno = director_films['return'].sum()
    return {
        "director": nombre_director,
        "total_retorno": total_retorno,
        "films": films_info
    }