

WITH x AS (
    SELECT 
        title.basics.tconst, COUNT(title.principals.nconst) as numOfActors
    FROM
        title.principals
    JOIN title.basics ON title.basics.tconst = title.principals.tconst
    WHERE title.basics.titleType = 'movie'
    GROUP BY title.basics.tconst
)
SELECT 
    title.akas.title, MAX(numOfActors) as numOfActors
FROM 
    x
JOIN title.akas ON title.akas.titleid = x.tconst
GROUP BY title.akas.title