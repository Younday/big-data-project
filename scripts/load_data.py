import psycopg2

conn = psycopg2.connect("host=localhost dbname=postgres user=unicorn_user password=magical_password")

# Create sample table
cur = conn.cursor()
cur.execute("""
    CREATE TABLE users(
    id integer PRIMARY KEY,
    email text,
    name text,
    address text
)
""")

#with open('data/user_accounts.csv', 'r') as f:
#    reader = csv.reader(f)
#    next(reader) # Skip the header row.
#    for row in reader:
#        cur.execute(
#        "INSERT INTO users VALUES (%s, %s, %s, %s)",
#        row
#    )
conn.commit()
