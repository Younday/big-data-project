

WITH top100 AS (
    SELECT 
        *
    FROM
        title.basics
    LEFT JOIN title.ratings ON title.ratings.tconst = title.basics.tconst
    WHERE title.basics.titleType = 'movie'
    ORDER BY averageRating DESC
    LIMIT 100
)
SELECT startYear, COUNT(startYear) as count
FROM top100
GROUP BY startYear
ORDER BY count DESC
LIMIT 1