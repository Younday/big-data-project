CREATE TABLE title_basics(
    tconst text PRIMARY KEY,
    titleType text,
    primaryTitle text,
    originalTitle text,
    isAdult BOOLEAN,
    startYear CHAR(4),
    endYear CHAR(4),
    runtimeMinutes int,
    genres text
)