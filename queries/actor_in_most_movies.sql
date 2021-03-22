SELECT name, MAX(count) FROM
    (SELECT 
        name, COUNT(title.principals.nconst) as count
    FROM title.principals
    LEFT JOIN name.basics ON name.basics.nconst = title.principals.nconst
    LEFT JOIN title.akas ON title.akas.titleId = principals.tconst
    LEFT JOIN title.basics ON title.basics.tconst = title.akas.titleId
    WHERE 
        title.basics.titleType = "movie"
    GROUP BY
        name)