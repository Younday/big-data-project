SELECT CONCAT  (table_schema, '.', table_name) AS "table"
FROM information_schema.tables 
WHERE table_schema = 'title' or table_schema = 'name' 
ORDER BY table_schema,table_name