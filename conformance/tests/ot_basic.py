

def test_count(O):
    errors = []
    
    O.query().addV("vertex1").property("field1", "value1").property("field2", "value2").execute()
    O.query().addV("vertex2").execute()
    O.query().addV("vertex3").property("field1", "value3").property("field2", "value4").execute()
    O.query().addV("vertex4").execute()

    O.query().V("vertex1").addE("friend").to("vertex2").execute()
    O.query().V("vertex2").addE("friend").to("vertex3").execute()
    O.query().V("vertex2").addE("parent").to("vertex4").execute()

    count = 0
    for i in O.query().V().execute():
        count += 1
    if count != 4:
        errors.append("Fail: O.query().V()")

    count = 0
    for i in O.query().E().execute():
        count += 1
    if count != 3:
        errors.append("Fail: O.query().E()")
    
    count = 0
    for i in O.query().V("vertex1").outgoing().execute():
        count += 1
    if count != 1:
        errors.append("Fail: O.query().V(\"vertex1\").outgoing()")
        
    count = 0
    for i in O.query().V("vertex1").outgoing().outgoing().has("field2", "value4").incoming().execute():
        count += 1
    if count != 1:
        errors.append("""O.query().V("vertex1").outgoing().outgoing().has("field1", "value4")""")
    
    return errors


def test_duplicate(O):
    errors = []
    O.query().addV("vertex1").execute()
    O.query().addV("vertex1").execute()
    O.query().addV("vertex2").execute()
    
    O.query().V("vertex1").addE("friend").to("vertex2").execute()
    O.query().V("vertex1").addE("friend").to("vertex2").execute()
    
    if O.query().V().count().first()['int_value'] != 2:
        errors.append("duplicate vertex add error")
    
    if O.query().E().count().first()['int_value'] != 2:
        errors.append("duplicate edge add error")
    return errors
