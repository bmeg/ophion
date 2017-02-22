

def test_sum(O):
    errors = []

    O.query().addV("vertex1").property({
        "type" : "sample",
        "count" : {
            "AAA" : 1,
            "BBB" : 1,
            "CCC" : 10,
            "EEE" : 10
        }
    }).execute()

    O.query().addV("vertex2").property({
        "type" : "sample",
        "count" : {
            "AAA" : 1,
            "BBB" : 1,
            "DDD" : 20
        }
    }).execute()


    for i in O.query().V().values("count").execute():
        for k in i['struct'].keys():
            if k not in ["count"]:
                errors.append("Wrong value selection: %s" % (k) )


    for i in O.query().V().values("count").map("""
function(x) {
 sum = 0
 for (var key in x.count) {
   sum += x.count[key]
 }
 return { "sum" : sum }
}
""").execute():
        if i['struct']['sum'] != 22:
            errors.append("Bad Mapping function")
    
    for i in O.query().V().values("count").fold("""
function(x, y) {
 sum = 0
 for (var key in x.count) {
   sum += x.count[key]
 }
 return { "sum" : sum }
}
""").execute():
        print i

    return list(set(errors))
