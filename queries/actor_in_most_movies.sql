SELECT primaryName, MAX(P.count) FROM
    (SELECT 
        primaryName, COUNT(title.principals.nconst) as count
    FROM title.principals
    LEFT JOIN name.basics ON name.basics.nconst = title.principals.nconst
    LEFT JOIN title.akas ON title.akas.titleId = principals.tconst
    LEFT JOIN title.basics ON title.basics.tconst = title.akas.titleId
    WHERE 
        title.basics.titleType = 'movie' GROUP BY primaryName) P
    GROUP BY
        primaryName