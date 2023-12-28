from uuid import UUID

from pydantic import BaseModel

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
