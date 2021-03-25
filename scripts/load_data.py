import psycopg2
import csv
import time
from tqdm import tqdm
from pymongo import MongoClient
import pandas as pd
import sys

# Postgres connection
try:
    postgres_conn = psycopg2.connect("host=localhost dbname=postgres user=unicorn_user password=magical_password")
    postgres_cur = postgres_conn.cursor()
    print("Connected successfully to Postgres")
except:
    print("Error 1: ", sys.exc_info()[1])

# MongoDB connection
try:
    mongo_conn = MongoClient('mongodb://root:rootpassword@localhost:27017/')
    print("Connected successfully to MongoDB")
except:
    print("Error 2: ", sys.exc_info()[1])

# connecting or switching to the database
mongo_db = None


def create_db_mongo():
    global mongo_db
    
    mongo_db = mongo_conn["imdb"]

def create_table_postgres():
    # Execute the query in create_table.sql
    with open('../queries/create_table.sql','r') as inserts:
        query = inserts.read()
    try:
        postgres_cur.execute(query)
    except:
       print("Error 3: ", sys.exc_info()[1])

def insert_data_mongo():
    collection = mongo_db.title_basics 
    
    # Read csv to dataframe and convert to dict (json like)
    df = pd.read_csv("../data/title.basics.tsv",sep='\t')
    mongo_records = df.to_dict('records')
    
    # Start timer
    t0 = time.time()
    
    try:
        # Json to database
        collection.insert_many(mongo_records)
    except:
        print("Error 4:", sys.exc_info()[1])
    
    # End timer
    t1 = time.time()
    
    return t1-t0

def insert_data_postgres():
    tsv_file = open("../data/title.basics.tsv", 'r')

    # Skip header file
    next(tsv_file)

    # Start timer
    t0 = time.time()

    try:
        # File to database
        postgres_cur.copy_from(tsv_file, 'title_basics', sep="\t")
        postgres_conn.commit()
    except:
        print("Error 5: ", sys.exc_info()[1])

    # End timer
    t1 = time.time()
    
    return t1-t0

def clear_mongo():
    try:
        mongo_conn.drop_database('imdb')
    except:
        print("Error 6:", sys.exc_info()[1])

def clear_postgres():
    with open('../queries/drop_table.sql','r') as inserts:
        query = inserts.read()
    try:
        postgres_cur.execute(query)
        rows = postgres_cur.fetchall()
        for row in rows:
            postgres_cur.execute("drop table " + row[1] + " cascade")
    except:
        print("Error 7: ", sys.exc_info()[1])



def main():
    clear_postgres()
    clear_mongo()
    
    create_table_postgres()
    create_db_mongo()

    total_postgres = insert_data_postgres()
    total_mongo = insert_data_mongo()
    print("Total time loading title.basics table for Postgres: ", total_postgres)
    print("Total time loading title.basics table for MongoDB: ", total_mongo)



if __name__ == "__main__":
    main()