from bson.code import Code
import time
from pprint import pprint

def actor_in_most_movies(db):
    col = db["title.basics"]
    t0 = time.time()
    result = col.aggregate(
        [
            {"$match": {"titleType": "movie"}},
            {
                 "$lookup": {
                     "from": "title.principals",
                     "localField": "tconst",
                     "foreignField": "tconst",
                     "as": "actors",
                 }
             },
             {"$unwind": "$actors"},
             {"$group": {"_id": "$actors.nconst", "count": {"$sum": 1}}},
             {"$sort": {"count": -1}},
             {"$limit": 1},
             {
                 "$lookup": {
                     "from": "name.basics",
                     "localField": "_id",
                     "foreignField": "nconst",
                     "as": "actor",
                 }
             },
        ]
    )

    t1 = time.time()

    return len(list(result)), t1-t0


def stdev_movie_runtimes(db):
    col = db["title.basics"]
    t0 = time.time()
    stdev = col.aggregate(
        [
            {
                "$group": {
                    "_id": "_id",
                    "Standard_dev_movie_runtimes": {"$stdDevPop": "$runtimeMinutes"},
                }
            }
        ]
    )
    t1 = time.time()

    return len(list(stdev)), t1-t0


def num_movies_with_rating_higher_than_95(db):
    col = db["title.ratings"]

    t0 = time.time()
    result = col.aggregate(
        [
            {"$match": {"averageRating": {"$gt": 9.5}}},
            {"$count": "Higher_than_95_count"},
        ]
    )
    t1 = time.time()

    return len(list(result)), t1-t0


def num_movies_with_death_actors(db):
    col = db["name.basics"]

    t0 = time.time()
    result = col.aggregate(
        [
            {"$match": {"deathYear": {"$not": {"$eq": "\\N"}}}},
            {
                "$lookup": {
                    "from": "title.principals",
                    "localField": "nconst",
                    "foreignField": "nconst",
                    "as": "movies",
                }
            },
            {"$match": {"movies": {"$not": {"$size": 0}}}},
            {"$unwind": "$movies"},
            {"$group": {"_id": "$movies.tconst", "count": {"$sum": 1}}},
            {
                "$lookup": {
                    "from": "title.basics",
                    "localField": "_id",
                    "foreignField": "tconst",
                    "as": "movie_info",
                }
            },
            {"$unwind": "$movie_info"},
            {"$match": {"movie_info.titleType": "movie"}},
            {"$count": "Movies_with_death_actors_count"},
        ]
    )
    t1 = time.time()

    return len(list(result)), t1-t0


def kate_and_leo(db):
    col = db["name.basics"]

    t0 = time.time()
    result = col.aggregate(
        [
            {
                "$match": {
                    "$or": [
                        {"primaryName": "Leonardo DiCaprio"},
                        {"primaryName": "Kate Winslet"},
                    ]
                }
            },
            {
                "$lookup": {
                    "from": "title.principals",
                    "localField": "nconst",
                    "foreignField": "nconst",
                    "as": "movies",
                }
            },
            {"$unwind": "$movies"},
            {"$group": {"_id": "$movies.tconst", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gte": 2}}},
            {"$group": {"_id": "_id", "Kate_Leo_Same_Movie": {"$sum": 1}}},
        ]
    )
    t1 = time.time()
    
    return len(list(result)), t1-t0


def top_10_languages(db):
    col = db["title.basics"]

    t0 = time.time()
    result = col.aggregate(
        [
            {"$match": {"titleType": "movie"}},
            {
                "$lookup": {
                    "from": "title.akas",
                    "localField": "tconst",
                    "foreignField": "titleId",
                    "as": "versions",
                }
            },
            {"$unwind": "$versions"},
            {"$group": {"_id": "$versions.language", "used": {"$sum": 1}}},
            {"$sort": {"used": -1}},
            {"$limit": 10},
        ]
    )
    t1 = time.time()
    return len(list(result)), t1-t0


def movie_most_actors(db):
    col = db["title.basics"]

    t0 = time.time()

    result = col.aggregate(
        [
            {"$match": {"titleType": "movie"}},
            {
                "$lookup": {
                    "from": "title.principals",
                    "localField": "tconst",
                    "foreignField": "tconst",
                    "as": "actors",
                }
            },
            {"$unwind": "$actors"},
            {"$group": {"_id": "$actors.tconst", "amount_of_actors": {"$sum": 1}}},
            {"$sort": {"amount_of_actors": -1}},
            {"$limit": 1},
            {
                "$lookup": {
                    "from": "title.basics",
                    "localField": "_id",
                    "foreignField": "tconst",
                    "as": "movie",
                }
            },
        ]
    )
    t1 = time.time()

    return len(list(result)), t1-t0


def year_most_top_100(db):
    col = db["title.ratings"]

    t0 = time.time()

    result = col.aggregate(
        [
            {"$sort": {"averageRating": -1}},
            {
                "$lookup": {
                    "from": "title.basics",
                    "localField": "tconst",
                    "foreignField": "tconst",
                    "as": "movies",
                }
            },
            {"$unwind": "$movies"},
            {"$match": {"movies.titleType": "movie"}},
            {"$limit": 100},
            {
                "$group": {
                    "_id": "$movies.startYear",
                    "amount_of_top_100_movies": {"$sum": 1},
                }
            },
            {"$sort": {"amount_of_top_100_movies": -1}},
            {"$limit": 1},
        ], allowDiskUse=True
    )

    t1 = time.time()

    return len(list(result)), t1-t0


def avg_runtime_all_actors_death(db):
    col = db["title.principals"]
    t0 = time.time()
    result = col.aggregate(
        [
            {
                "$lookup": {
                    "from": "name.basics",
                    "localField": "nconst",
                    "foreignField": "nconst",
                    "as": "name",
                }
            },
            {
                "$group": {
                    "_id": "$tconst",
                    "actors": {"$push": "$name"},
                    "total": {"$sum": 1},
                }
            },
            {
                "$unwind": "$actors",
            },
            #{
            #    "$match": {
            #        "$and": [
            #            {"actors": {"$not": {"$eq": []}}},
            #            {"actors.deathYear": {"$not": {"$eq": "\\N"}}},
            #        ]
            #    }
            #},
            #{
            #    "$group": {
            #        "_id": "$_id",
            #        "actors": {"$push": "$actors"},
            #        "total": {"$first": "$total"},
            #        "death": {"$sum": 1},
            #    }
            #},
            #{"$match": {"$expr": {"$eq": ["$death", "$total"]}}},
            #{
            #    "$lookup": {
            #        "from": "title.basics",
            #        "localField": "_id",
            #        "foreignField": "tconst",
            #        "as": "movie",
            #    }
            #},
            #{"$unwind": "$movie"},
            #{"$match": {"movie.titleType": "movie"}},
            #{
            #    "$group": {
            #        "_id": "_id",
            #        "Average_movietime_with_death_actors": {
            #            "$avg": "$movie.runtimeMinutes"
            #        },
            #    }
            #}
        ], allowDiskUse=True
    )
    t1 = time.time()

    return len(list(result)), t1-t0

def genre_most_movies(db):
    col = db["title.basics"]
    t0 = time.time()
    result = col.aggregate(
        [
            {"$match": {"titleType": "movie"}},
            {
                "$group": {
                    "_id": "$genres",
                    "movies_in_genre": {"$sum": 1},
                }
            },
            {"$sort": {"movies_in_genre": -1}},
            {"$limit": 1},
        ]
    )
    t1 = time.time()
    
    return len(list(result)), t1-t0