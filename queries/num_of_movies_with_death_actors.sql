WITH x AS (
    SELECT 
        title.principals.tconst, COUNT(nconst) as counts
    FROM 
        title.principals
    LEFT JOIN title.basics ON title.basics.tconst = title.principals.tconst
    WHERE
        title.principals.nconst IN (
            SELECT 
                nconst
            FROM
                name.basics
            WHERE name.basics.deathYear is not Null
    )
        AND
        title.basics.titleType = 'movie'
    GROUP BY title.principals.tconst
)
SELECT COUNT(P.tconst) as numMovies
FROM (
    SELECT 
        title.principals.tconst, x.counts
    FROM 
        title.principals

    JOIN x ON x.tconst = title.principals.tconst
    GROUP BY title.principals.tconst, x.counts
    HAVING
        x.counts = COUNT(title.principals.nconst)
) P