from __future__ import absolute_import, print_function

import sys
import json
import urllib2
import traceback


class Ophion:
    def __init__(self, host):
        self.host = host
        self.url = host + "/vertex/query"

    # entry point
    def query(self):
        return OphionQuery(self)

    # conditions
    def eq(self, v):
        return {'eq': v}

    def neq(self, v):
        return {'neq': v}

    def gt(self, v):
        return {'gt': v}

    def gte(self, v):
        return {'gte': v}

    def lt(self, v):
        return {'lt': v}

    def lte(self, v):
        return {'lte': v}

    def between(self, lower, upper):
        return {'between': {'lower': lower, 'upper': upper}}

    def inside(self, lower, upper):
        return {'inside': {'lower': lower, 'upper': upper}}

    def outside(self, lower, upper):
        return {'outside': {'lower': lower, 'upper': upper}}

    def within(self, values):
        return {'within': values}
        # return {'within': {'values': values}}

    def without(self, values):
        return {'without': values}
        # return {'without': {'values': values}}

    # for match subqueries
    def mark(self, label):
        return self.query().mark(label)

    # remote calls
    def vertex(self, gid):
        url = self.host + "/vertex/find/" + gid
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        request = urllib2.Request(url, headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        return json.loads(result)

    def execute(self, query):
        def loadJson(s):
            if len(s) > 0:
                return json.loads(s)
            else:
                return {}

        try:
            payload = query.render()

            # streaming request attempt
            # response = requests.post(self.url, data=payload, stream=True)
            # v = []
            # # if response.encoding is None:
            # #     response.encoding = 'utf-8'
            # for item in response.iter_lines(decode_unicode=True):
            #     v.append(item)
            # return v

            headers = {'Content-Type': 'application/json',
                       'Accept': 'application/json'}
            request = urllib2.Request(self.url, payload, headers=headers)
            response = urllib2.urlopen(request)
            result = response.read()
            return map(loadJson, result.rstrip().split("\n"))
        except Exception as e:
            traceback.print_exc()
            return {
                'error': {
                    'system': sys.exc_info()[0],
                    'exception': e
                }
            }


def wrapValue(value):
    v = value
    if isinstance(value, int):
        v = {'n': value}
    elif isinstance(value, float):
        v = {'r': value}
    elif isinstance(value, str):
        v = {'s': value}
    elif isinstance(value, list):
        v = map(wrapValue, value)
    elif isinstance(value, dict):
        v = {k: wrapValue(v) for k, v in value.iteritems()}
    return v


class OphionQuery:
    def __init__(self, parent=None):
        self.query = []
        self.parent = parent

    # traversals
    def incoming(self, labels=None):
        if labels is None:
            self.query.append({'in': []})
        else:
            if not isinstance(labels, list):
                labels = [labels]
            self.query.append({'in': {'labels': labels}})
        return self

    def outgoing(self, labels=None):
        if labels is None:
            self.query.append({'out': []})
        else:
            if not isinstance(labels, list):
                labels = [labels]
            self.query.append({'out': {'labels': labels}})
        return self

    def inEdge(self, labels=None):
        if labels is None:
            self.query.append({'inEdge': []})
        else:
            if not isinstance(labels, list):
                labels = [labels]
            self.query.append({'inEdge': {'labels': labels}})
        return self

    def outEdge(self, labels=None):
        if labels is None:
            self.query.append({'outEdge': []})
        else:
            if not isinstance(labels, list):
                labels = [labels]
            self.query.append({'outEdge': {'labels': labels}})
        return self

    def inVertex(self):
        self.query.append({'inVertex': True})
        return self

    def outVertex(self):
        self.query.append({'outVertex': True})
        return self

    # traversal manipulation
    def mark(self, label):
        if not isinstance(label, list):
            label = [label]
        self.query.append({'as': {'labels': label}})
        return self

    def select(self, labels):
        if not isinstance(labels, list):
            labels = [labels]
        self.query.append({'select': {'labels': labels}})
        return self

    def by(self, label):
        self.query.append({'by': {'key': label}})
        return self

    def label(self, *args):
        # if len(args) == 0:
        #     self.query.append({'label': })
        # if not isinstance(labels, list):
        #     labels = [labels]
        # self.query.append({'label': {'labels': labels}})
        self.query.append({'label': {}})
        return self

    def values(self, labels):
        if not isinstance(labels, list):
            labels = [labels]
        self.query.append({'values': {'labels': labels}})
        return self

    def limit(self, l):
        self.query.append({'limit': l})
        return self

    def order(self, o, asc):
        self.query.append({'order': {'key': o, 'ascending': asc}})
        return self

    def range(self, begin, end):
        self.query.append({'range': {'lower': begin, 'upper': end}})
        return self

    def count(self):
        self.query.append({'count': True})
        return self

    def dedup(self):
        self.query.append({'dedup': []})
        return self

    def path(self):
        self.query.append({'path': True})
        return self

    def aggregate(self, label):
        self.query.append({'aggregate': label})
        return self

    def group(self, bys):
        self.query.append({'group': {'bys': map(lambda by: {'key': by})}})
        return self

    def groupCount(self, label=None):
        if label is None:
            self.query.append({'groupCount': {}})
        else:
            self.query.append({'groupCount': {'key': label}})
        return self

    def satisfies(self, condition):
        self.query.append({'is': wrapValue(condition)})
        return self

    def has(self, key, value=None):
        outer = {'key': key}
        if value is not None:
            v = wrapValue(value)
            outer = {'key': key}
            if isinstance(value, dict):
                outer['condition'] = v
            else:
                outer['value'] = v
        self.query.append({'has': outer})
        return self

    def hasNot(self, key):
        self.query.append({'hasNot': key})
        return self

    def match(self, queries):
        self.query.append({'match': {'queries': queries}})
        return self

    def searchVertex(self, search, term=None):
        opts = {'search': search}
        if term is not None:
            opts['term'] = term
        self.query.append({'searchVertex': opts})
        return self

    def searchEdge(self, search, term=None):
        opts = {'search': search}
        if term is not None:
            opts['term'] = term
        self.query.append({'searchEdge': opts})
        return self

    # output
    def render(self):
        # output = {'query': self.query}
        def subsubrender(step):
            if isinstance(step, list) or isinstance(step, dict):
                if 'queries' in step:
                    return {'queries': map(lambda v: v.query, step['queries'])}
                else:
                    return step
            else:
                return step

        def subrender(step):
            return {k: subsubrender(v) for k, v in step.items()}

        outquery = map(subrender, self.query)
        return json.dumps(outquery)

    def execute(self):
        return self.parent.execute(self)


# FIND SAMPLES WITH A MUTATION IN BRAF
# O.query().match([
#     O.mark("gene").has("symbol", "BRAF"),
#     O.mark("gene").incoming("variantInGene").outgoing("variantInBiosample").mark("sample")
# ]).select(["gene", "sample"])


# SAMPLE x EXPRESSION matrix
# O.query().has("gid", "cohort:CCLE").outgoing("hasSample").mark("sample").\
#     incoming("expressionForSample").mark("expression").\
#     select(["sample", "expression"]).count().execute()


# SAMPLE x RESPONSE matrix
# O.query().has("gid", "cohort:CCLE").outgoing("hasSample").mark("sample").\
#     outEdge("responseToCompound").mark("response").\
#     select(["sample", "response"]).count().execute()

# FIND ALL SAMPLExEXPRESSIONxRESPONSE TRIPLES FOR CCLE COHORT
# O.query().has("gid", "cohort:CCLE").outgoing("hasSample").match([
#     O.mark("sample").incoming("expressionForSample").values(['serializedExpresion']).mark("expression"),
#     O.mark("sample").outEdge("responseToCompound").mark("response")
# ]).select(["sample", "expression", "response"]).limit(1)
