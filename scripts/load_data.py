import psycopg2
import csv
import time
from tqdm import tqdm
from pymongo import MongoClient
import pandas as pd
import sys
import argparse
import io
import mongo_queries as mq
import numpy as np

class Range(object):
    def __init__(self, start, end):
        self.start = start
        self.end = end
    def __eq__(self, other):
        return self.start <= other <= self.end

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

def insert_data_mongo(data, table, limit=1.0):
    collection = mongo_db[table] 
    
    # Read csv to dataframe and convert to dict (json like)
    df = pd.read_csv("../data/" + data,sep='\t')
    length = df.shape[0]
    split = int(length * limit)
    df_f = df.head(split)

    mongo_records = df_f.to_dict('records')

    if (data == "title.basics.tsv"):
        for sub in mongo_records:
            if not (sub["runtimeMinutes"] == "\\N"):
                sub["runtimeMinutes"] = int(sub["runtimeMinutes"])
    
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

def insert_data_postgres(data, table, limit=1.0):
    if limit == 1.0:
        tsv_file = open("../data/" + data, 'r')
        next(tsv_file)
    else:
        tsv_file = open("../data/" + data, 'r')
        reader = csv.reader(tsv_file, delimiter='\t')
        next(reader, None)
        s = len(list(reader))
        size = s * limit
        out = io.StringIO()
        out_list = list(reader)[0:int(size)]
        for item in out_list:
            out.write(item.join("\t") + '\n')
        out.seek(0)
        tsv_file = out

    # Start timer
    t0 = time.time()

    try:
        # File to database
        postgres_cur.copy_from(tsv_file, table, sep="\t")
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
            postgres_cur.execute("drop table " + row[0] + " cascade")
    except:
        print("Error 7: ", sys.exc_info()[1])



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("limit", help="float between 0 and 1 to set percentage of data loading", type=float, choices=[Range(0.0, 1.0)])
    args = parser.parse_args()
    data = ['title.akas.tsv', 'title.basics.tsv', 'title.crew.tsv', 'title.episode.tsv', 'title.principals.tsv', 'title.ratings.tsv', 'name.basics.tsv']
    tables = ['title.akas', 'title.basics', 'title.crew', 'title.episode', 'title.principals', 'title.ratings', 'name.basics']

    clear_postgres()
    clear_mongo()
    
    create_table_postgres()
    create_db_mongo()

    for d, t in zip(data, tables):
        total_postgres = insert_data_postgres(d, t, limit=args.limit)
        print("Total time loading {} table for Postgres: {:>20f}".format(t, total_postgres))
        total_mongo = insert_data_mongo(d, t)
        print("Total time loading {} table for MongoDB: {:>20f}".format(t, total_mongo))



if __name__ == "__main__":
    main()
    
    mongo_db = mongo_conn["imdb"]

    print(mq.actor_in_most_movies(mongo_db))
    print(mq.kate_and_leo(mongo_db))
    print(mq.stdev_movie_runtimes(mongo_db))
    print(mq.num_movies_with_rating_higher_than_95(mongo_db))
    print(mq.num_movies_with_death_actors(mongo_db))
    print(mq.top_10_languages(mongo_db))
    print(mq.movie_most_actors(mongo_db))
    print(mq.year_most_top_100(mongo_db))
    print(mq.avg_runtime_all_actors_death(mongo_db))