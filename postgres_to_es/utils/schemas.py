from uuid import UUID
from datetime import datetime

from pydantic import BaseModel


class PGObject(BaseModel):
    id: UUID
    modified_at: datetime


class Person(BaseModel):
    id: str
    full_name: str


class ESFilmwork(BaseModel):
    id: UUID
    imdb_rating: float | None
    genre: list
    title: str
    description: str | None
    director: str | None
    actors_names: list
    writers_names: list
    actors: list[Person]
    writers: list[Person]
