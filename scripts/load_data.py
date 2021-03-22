import psycopg2
import csv
import time
from tqdm import tqdm

conn = psycopg2.connect("host=localhost dbname=postgres user=unicorn_user password=magical_password")
cur = conn.cursor()


def create_tables():
    with open('../queries/create_table.sql','r') as inserts:
        query = inserts.read()
    cur.execute(query)

def read_data():

    tsv_file = open("../data/title.basics.tsv")
    read_tsv = csv.reader(tsv_file, delimiter="\t")

    t0 = time.time()
    next(read_tsv) # Skip the header row.
    for row in tqdm(read_tsv):
        if row[7] == '\\N':
            row[7] = 0
        cur.execute(
    "INSERT INTO title_basics VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        row)
    conn.commit()
    t1 = time.time()
    return t1-t0

def main():
    create_tables()
    total = read_data()
    print("Total time loading title.basics table: %s", total)

if __name__ == "__main__":
    main()