from bson.code import Code
from pprint import pprint

# movies moeten worden toegevoegd als criteria
# in SQL niet check op actor, hier alleen in most movies wel?!
# languages skipt sql de \n?


def actor_in_most_movies(db):
    col = db["title.basics"]

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

    return list(result)


def stdev_movie_runtimes(db):
    col = db["title.basics"]

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

    return list(stdev)


def num_movies_with_rating_higher_than_95(db):
    col = db["title.ratings"]

    result = col.aggregate(
        [
            {"$match": {"averageRating": {"$gt": 9.5}}},
            {"$count": "Higher_than_95_count"},
        ]
    )

    return list(result)


def num_movies_with_death_actors(db):
    col = db["name.basics"]

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
            {"$count": "Movies_with_death_actors_count"},
        ]
    )

    return list(result)


def kate_and_leo(db):
    col = db["name.basics"]

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

    return list(result)


def top_10_languages(db):
    col = db["title.basics"]

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

    return list(result)


def movie_most_actors(db):
    col = db["title.basics"]

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

    return list(result)


def year_most_top_100(db):
    col = db["title.ratings"]

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
        ]
    )

    return list(result)


def avg_runtime_all_actors_death(db):
    col = db["title.principals"]

    result = col.aggregate(
        [
            {
                "$group": {
                    "_id": "$tconst",
                    "actors": {"$push": "$nconst"},
                    "total": {"$sum": 1},
                }
            },
            {
                "$unwind": "$actors",
            },
            {
                "$lookup": {
                    "from": "name.basics",
                    "localField": "actors",
                    "foreignField": "nconst",
                    "as": "name",
                }
            },
            {
                "$match": {
                    "$and": [
                        {"name": {"$not": {"$eq": []}}},
                        {"name.deathYear": {"$not": {"$eq": "\\N"}}},
                    ]
                }
            },
            {
                "$group": {
                    "_id": "$_id",
                    "actors": {"$push": "$actors"},
                    "total": {"$first": "$total"},
                    "death": {"$sum": 1},
                }
            },
            {"$match": {"$expr": {"$eq": ["$death", "$total"]}}},
            {
                "$lookup": {
                    "from": "title.basics",
                    "localField": "_id",
                    "foreignField": "tconst",
                    "as": "movie",
                }
            },
            {"$unwind": "$movie"},
            {
                "$group": {
                    "_id": "_id",
                    "Average_movietime_with_death_actors": {
                        "$avg": "$movie.runtimeMinutes"
                    },
                }
            },
        ]
    )

    return list(result)
