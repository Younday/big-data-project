import psycopg2
import csv
import time
from tqdm import tqdm
from pymongo import MongoClient
import pandas as pd
import sys
import argparse
import io


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

def run_query(query):
    # Execute the query in create_table.sql
    with open('../queries/' + query,'r') as inserts:
        query = inserts.read()
    t0 = time.time()
    postgres_cur.execute(query)
    t1 = time.time()
    rows = postgres_cur.fetchall()
    return t1-t0, len(rows)


def main():
    queries = ['actor_in_most_movies.sql', 'avg_runtime_of_movies_where_all_actors_dies.sql', 'Kate_and_Leonardo_in_same_movie.sql', 
    'num_movies_with_rating_higher_tahn_95.sql', 'num_of_movies_with_death_actors.sql', 'stdev_movie_runtimes.sql', 
    'top_ten_most_languages.sql', 'Which_genres_contain_most_movies.sql', 'Which_movie_has_most_actors.sql', 'which_year_most_top100_movies.sql']

    for query in queries:
        run_time, rows = run_query(query)
        print("Total time loading query {}: {}  returns amount of rows: {}".format(query, run_time, rows))

if __name__ == "__main__":
    main()