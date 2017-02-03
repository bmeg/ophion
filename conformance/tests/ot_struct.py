

def test_struct(O):
    errors = []
    print O.query().addV("vertex1").property("field1", {"test" : 1, "value" : False}).render()
    O.query().addV("vertex1").property("field1", {"test" : 1, "value" : False}).execute()
    
    for i in O.query().V().execute():
        print i