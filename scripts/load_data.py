import psycopg2
import csv
import time
from tqdm import tqdm
from pymongo import MongoClient
import pandas as pd

# Postgres connection
conn = psycopg2.connect("host=localhost dbname=postgres user=unicorn_user password=magical_password")
cur = conn.cursor()

# MongoDB connection
try:
    connect = MongoClient('mongodb://root:rootpassword@localhost:27017/')
    print("Connected successfully!!!")
except:
    print("Could not connect to MongoDB")

# connecting or switching to the database
db = connect.demoDB


def create_tables():
    with open('../queries/create_table.sql','r') as inserts:
        query = inserts.read()
    try:
        cur.execute(query)
    except psycopg2.errors.DuplicateTable:
        print("Table already exists in Postgres")

def insert_data_mongo():
    collection = db.title_basics
    df = pd.read_csv("../data/title.basics.tsv",sep='\t')
    mongo_records = df.to_dict('records')
    t0 = time.time()
    collection.insert_many(mongo_records)
    t1 = time.time()
    return t1-t0

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
    total_postgres = read_data()
    total_mongo = insert_data_mongo()
    print("Total time loading title.basics table for Postgres: %s", total_postgres)
    print("Total time loading title.basics table for MongoDB: %s", total_mongo)

if __name__ == "__main__":
    main()