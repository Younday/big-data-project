SELECT
    genres, COUNT(tconst) as num
FROM
    title.basics
WHERE title.basics.titleType = 'movie'
GROUP BY title.basics.genres
ORDER BY COUNT(tconst) DESC
LIMIT 1