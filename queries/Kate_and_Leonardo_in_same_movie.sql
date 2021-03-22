SELECT COUNT(count) FROM (SELECT 
        tconst, COUNT(tconst) as count
    FROM
        title.principals
    LEFT JOIN name.basics ON name.basics.nconst = title.principals.nconst
    WHERE name.basics.name = 'Leonardo DiCaprio' or name.basics.name = 'Kate Winslet'
    GROUP BY tconst) 
WHERE count = 2