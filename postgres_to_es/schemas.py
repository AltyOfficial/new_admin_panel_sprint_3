from uuid import UUID

from pydantic import BaseModel


class Filmwork(BaseModel):
    title: str


class Genre(BaseModel):
    name: str
    description: str


class Person(BaseModel):
    id: str
    full_name: str


class GenreFilmwork(BaseModel):
    pass


class PersonFilmwork(BaseModel):
    role: str


class ESFilmwork(BaseModel):
    id: UUID
    imdb_rating: float
    genre: str
    title: str
    description: str
    director: str
    actors_names: str
    writers_names: str
    actors: list[PersonFilmwork]
    writers: list[PersonFilmwork]
