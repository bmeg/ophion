import json
import urllib2

class BMEG():    
    def __init__(self):
      base = "http://10.96.11.144"  
      self.url = base+"/query/{}"
      self.schema_url = "{}/schema/protograph".format(base)

    def query(self, label, query=[["where", {}], ["limit", 10]]):
        url = self.url.format(label)
        payload = json.dumps(query)
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request(url, payload, headers=headers)
        response = urllib2.urlopen(request)
        for result in response:
            try:
                yield(json.loads(result))
            except ValueError, e:
                print "Can't decode: %s" % result
                raise e

    def schema(self):
        request = urllib2.Request(self.schema_url)
        response = urllib2.urlopen(request)
        body = response.read()
        return json.loads(body)



# O
# .V()
# .label('Individual')
# .aggregate({
#     'genderAges': {
#         'histogram': {
#             'group': ['gender'],
#             'field': 'age',
#             'interval': 10
#         }
#     }
# })

# {'genderAges': {
#     'MALE': {
#         10: 111,
#         20: 2717,
#         30: ...,
#     },
#     'FEMALE': {
#         10: 3294823,
#         20: 187394862394862394,
#         30: ...,
#     }
# }}

# [['mark', 'individual'],
#  ['to', 'variantCall', 'Variant'],
#  ['to', 'variantIn', 'Gene'],
#  ['where', {'symbol': 'BRCA1'}],
#  ['mark', 'gene'],
#  ['select', ['individual', 'gene']]
#  ['aggregate', {
#      'genesPerIndividual': {'terms': {'field': 'individual'}},
#      'individualsPerGene': {'terms': {'field': 'gene'}}
#  }]]

# [['where']]

# {'genesPerIndividual': {
#     'Jack': 1818181,
#     'Mary': 23846,
#     'Joe': ....
# }}
