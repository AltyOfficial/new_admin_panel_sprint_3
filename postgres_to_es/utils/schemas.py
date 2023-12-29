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
    genre: str | None
    title: str
    description: str | None
    director: str | None
    actors_names: str
    writers_names: str
    actors: list[Person]
    writers: list[Person]
