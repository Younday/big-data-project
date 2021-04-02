SELECT 
    STDDEV(runtimeMinutes) as StandardDeviation
FROM
    title.basics
WHERE
    titleType = 'movie'