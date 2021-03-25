SELECT 
    STDEV(runtimeMinutes) as StandardDeviation
FROM
    title.basics
WHERE
    titleType = 'movie'