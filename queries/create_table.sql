CREATE SCHEMA IF NOT EXISTS title;
CREATE SCHEMA IF NOT EXISTS name;

CREATE TABLE title.basics(
    tconst text PRIMARY KEY,
    titleType text,
    primaryTitle text,
    originalTitle text,
    isAdult BOOLEAN,
    startYear CHAR(4),
    endYear CHAR(4),
    runtimeMinutes int,
    genres text
);

CREATE TABLE title.akas (
    titleId text,
    ordering int,
    title text,
    region text,
    language text,
    types text,
    attributes text,
    isOriginalTitle BOOLEAN,
    primary key (titleId, ordering)
);

CREATE TABLE title.crew(
    tconst text PRIMARY KEY,
    directors text,
    writers text
);

CREATE TABLE title.episode(
    tconst text PRIMARY KEY,
    parentTconst text,
    seasonNumber int,
    episodeNumber int
);

CREATE TABLE title.principals(
    tconst text,
    ordering int,
    nconst text,
    category text,
    job text,
    characters text,
    primary key (tconst, ordering, nconst)
);

CREATE TABLE title.ratings(
    tconst text PRIMARY KEY,
    averageRating FLOAT(2),
    numVotes int
);

CREATE TABLE name.basics(
    nconst text PRIMARY KEY,
    primaryName text,
    birthYear CHAR(4),
    deathYear CHAR(4),
    primaryProfession text,
    knownForTitles text
);