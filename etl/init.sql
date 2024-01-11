CREATE SCHEMA IF NOT EXISTS content;

SET search_path to content;

CREATE TABLE IF NOT EXISTS content.filmwork (
    id UUID PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    type TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE,
    modified_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS content.genre (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE,
    modified_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS content.person (
    id UUID PRIMARY KEY,
    full_name TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE,
    modified_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS content.genre_filmwork (
    id UUID PRIMARY KEY,
    genre_id UUID NOT NULL,
    filmwork_id UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT fk_gf_genre_id
        FOREIGN KEY (genre_id)
        REFERENCES content.genre (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_gf_filmwork_id
        FOREIGN KEY (filmwork_id)
        REFERENCES content.filmwork (id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS content.person_filmwork (
    id UUID PRIMARY KEY,
    person_id UUID NOT NULL,
    filmwork_id UUID NOT NULL,
    role TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT fk_pf_person_id
        FOREIGN KEY (person_id)
        REFERENCES content.person (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_pf_filmwork_id
        FOREIGN KEY (filmwork_id)
        REFERENCES content.filmwork (id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS filmwork_creation_date_idx ON content.filmwork(creation_date);

CREATE UNIQUE INDEX IF NOT EXISTS filmwork_genre_idx ON content.genre_filmwork (filmwork_id, genre_id);

CREATE UNIQUE INDEX IF NOT EXISTS filmwork_person_role_idx ON content.person_filmwork (filmwork_id, person_id, role);
