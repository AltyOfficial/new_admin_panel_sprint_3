import datetime
import uuid
from dataclasses import dataclass


@dataclass
class UUIDMixin:
    id: uuid.uuid4


@dataclass
class TimestampMixin:
    created_at: datetime.datetime
    modified_at: datetime.datetime


@dataclass
class Filmwork(UUIDMixin, TimestampMixin):
    title: str
    description: str
    creation_date: datetime.date
    rating: float
    type: str

    def __post_init__(self):
        self._asdict = {
            'id': self.id,
            'title': self.title,
            'description': self.description,
            'creation_date': self.creation_date,
            'rating': self.rating,
            'type': self.type,
            'created_at': self.created_at,
            'modified_at': self.modified_at
        }


@dataclass
class Genre(UUIDMixin, TimestampMixin):
    name: str
    description: str

    def __post_init__(self):
        self._asdict = {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'created_at': self.created_at,
            'modified_at': self.modified_at
        }


@dataclass
class Person(UUIDMixin, TimestampMixin):
    full_name: str

    def __post_init__(self):
        self._asdict = {
            'id': self.id,
            'full_name': self.full_name,
            'created_at': self.created_at,
            'modified_at': self.modified_at
        }


@dataclass
class GenreFilmwork(UUIDMixin):
    genre_id: uuid.uuid4
    film_work_id: uuid.uuid4
    created_at: datetime.datetime

    def __post_init__(self):
        self._asdict = {
            'id': self.id,
            'genre_id': self.genre_id,
            'film_work_id': self.film_work_id,
            'created_at': self.created_at
        }


@dataclass
class PersonFilmwork(UUIDMixin):
    person_id: uuid.uuid4
    film_work_id: uuid.uuid4
    role: str
    created_at: datetime.datetime

    def __post_init__(self):
        self._asdict = {
            'id': self.id,
            'person_id': self.person_id,
            'film_work_id': self.film_work_id,
            'role': self.role,
            'created_at': self.created_at
        }
