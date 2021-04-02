SELECT
    COUNT(title.ratings.averageRating) num_movies
FROM
    title.ratings
WHERE title.ratings.averageRating > 9.5