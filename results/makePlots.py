import matplotlib.pyplot as plt

def read(pwd):
    result = []
    with open(pwd) as fiiile:
        for line in fiiile.readlines():
            result.append(float(line.split("\n")[0]))
    return result

nameConversion = {
    "25results": "Dataset at 25%",
    "50results": "Dataset at 50%",
    "75results": "Dataset at 75%",
    "100results": "Dataset at 100%"
}

labelsload = ["load" + str(i) for i in range(7)]
labelsquery = ["query" + str(i) for i in range(10)]
labelsload.extend(labelsquery)

xunits = [i + 0.5 for i in range(17)]
xbla = [i + 0.25 for i in range(17)]

for key in nameConversion:
    namePost = "Postgres/" + key + "Post" + ".input"
    nameMong = "Mongo/" + key + "Mong" + ".input" 
    resultsPost = read(namePost)
    resultsMong = read(nameMong)
    plt.bar(range(17), resultsPost, color="lightblue", width=0.45)
    plt.bar(xunits, resultsMong, color="lightgreen", width=0.45)
    plt.legend(["PostgreSQL", "MongoDB"])
    plt.title(nameConversion[key])
    plt.xticks(xbla, labelsload, rotation=45)
    plt.xlabel("query id")
    plt.ylabel("runtime (Seconds)")
    plt.show()

