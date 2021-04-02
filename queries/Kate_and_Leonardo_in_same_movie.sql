SELECT COUNT(P.count) FROM (SELECT 
        tconst, COUNT(tconst) as count
    FROM
        title.principals
    LEFT JOIN name.basics ON name.basics.nconst = title.principals.nconst
    WHERE name.basics.primaryName = 'Leonardo DiCaprio' or name.basics.primaryName = 'Kate Winslet'
    GROUP BY tconst) P
WHERE P.count = 2