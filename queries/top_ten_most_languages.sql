SELECT
    title.akas.language
FROM
    title.akas
LEFT JOIN title.basics ON title.basics.tconst = title.akas.titleId
WHERE title.basics.titleType = 'movie'
GROUP BY title.akas.language
ORDER BY COUNT(title.akas.language) DESC
LIMIT 10