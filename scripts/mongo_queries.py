from bson.code import Code

def actor_in_most_movies(db):
    # Group hier gebruiken om bij elkaar te krijgen
    col = db["title.principals"]

    actor_id = col.aggregate([{
        "$match": {"$or": [{"category": "actor"}, {"category": "actress"}]}
        },{
        "$sortByCount": "$nconst"
    }])

    col = db["name.basics"]

    result = col.find({"nconst": (list(actor_id))[0]["_id"]})

    return list(result)[0]["primaryName"]

def stdev_movie_runtimes(db):
    col = db["title.basics"]

    stdev = col.aggregate([{
        "$group":
        {
            "_id":"_id",
            "AverageValue": {"$avg": "runtimeMinutes"}
        }
    }])

    print(list(stdev))

    return 0

    


# def kate_and_leo(db):
#     col = db["name.basics"]

#     leo = col.find({"primaryName": "Leonardo DiCaprio"}) 
#     kate = col.find({"primaryName": "Kate Winslet"})

#     col = db["title.principals"]

#     list(leo)[0]["nconst"]
#     list(kate)[0]["nconst"]

#     movies = col.aggregate([{
#         "$match": {"$or": [{"nconst": list(kate)[0]["nconst"]}, {"nconst": list(leo)[0]["nconst"]}]}