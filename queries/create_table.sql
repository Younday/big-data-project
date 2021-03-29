CREATE TABLE title_basics(
    tconst text PRIMARY KEY,
    titleType text,
    primaryTitle text,
    originalTitle text,
    isAdult BOOLEAN,
    startYear CHAR(4),
    endYear CHAR(4),
    runtimeMinutes int,
    genres text []
);

CREATE TABLE title_akas (
    titleId text PRIMARY KEY,
    ordering int,
    title text,
    region text,
    language text,
    types text [],
    attributes text [],
    isOriginalTitle BOOLEAN
);

CREATE TABLE title_crew(
    tconst text PRIMARY KEY,
    directors text [],
    writers text []
);

CREATE TABLE title_episode(
    tconst text PRIMARY KEY,
    parentTconst text,
    seasonNumber int,
    episodeNumber int
);

CREATE TABLE title_principals(
    tconst text PRIMARY KEY,
    ordering int,
    nconst text,
    category text,
    job text,
    characters text
);

CREATE TABLE title_ratings(
    tconst text PRIMARY KEY,
    averageRating FLOAT(2),
    numVotes int
);

CREATE TABLE name_basics(
    nconst text PRIMARY KEY,
    primaryName text,
    birthYear CHAR(4),
    deathYear CHAR(4),
    primaryProfession text [],
    knownForTitles text []
);