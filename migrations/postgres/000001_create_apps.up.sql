CREATE TABLE apps (
    id UUID PRIMARY KEY,
    name VARCHAR NOT NULL CHECK (btrim(name) <> ''),
    publisher VARCHAR NOT NULL CHECK (btrim(publisher) <> ''),
    category VARCHAR NOT NULL CHECK (btrim(category) <> ''),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);
