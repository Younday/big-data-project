SELECT
    COUNT(title.ratings.averageRating) num_movies
FROM
    title.ratings
LEFT JOIN title.basics ON title.basics.tconst = title.ratings.tconst
WHERE title.basics.titleType = 'movie' AND title.ratings.averageRating > 9.5