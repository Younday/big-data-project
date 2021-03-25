WITH x AS (
    SELECT 
        title_principals.tconst, COUNT(nconst) as counts
    FROM 
        title_principals
    LEFT JOIN title_basics ON title_basics.tconst = title_principals.tconst
    WHERE
        title_principals.nconst IN (
            SELECT 
                nconst
            FROM
                name_basics
            WHERE name_basics.deathYear is not Null
    )
        AND
        title_basics.titleType = 'movie'
    GROUP BY title_principals.tconst
)
SELECT COUNT(tconst) as numMovies
FROM (
    SELECT 
        title_principals.tconst
    FROM 
        title_principals

    JOIN x ON x.tconst = title_principals.tconst
    GROUP BY title_principals.tconst
    HAVING
        x.counts = COUNT(title_principals.nconst)
)