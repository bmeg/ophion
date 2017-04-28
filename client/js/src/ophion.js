require('./callsync')
require('underscore')

function OphionQuery(parent) {
  var parent = parent
  var query = []
  
  function labels(l) {
    if (!l) {
      l = []
    } else if (_.isString(l)) {
      l = [l]
    } else if (!_.isArray(l)) {
      console.log("not something we know how to make labels out of:")
      console.log(l)
    }

    return {'labels': l}
  }

  function by(b) {
    if (_.isString(b)) {
      out = {'key': b}
    } else {
      out = {'query': b.query}
    }

    return out
  }

  var operations = {
    query: query,

    V: function(l) {
      query.push({'V': labels(l)})
      return this
    },
    
    E: function(l) {
      query.push({'E': labels(l)})
      return this
    },
    
    incoming: function(l) {
      query.push({'in': labels(l)})
      return this
    },

    outgoing: function(l) {
      query.push({'out': labels(l)})
      return this
    },

    inEdge: function(l) {
      query.push({'inEdge': labels(l)})
      return this
    },

    outEdge: function(l) {
      query.push({'outEdge': labels(l)})
      return this
    },

    inVertex: function(l) {
      query.push({'inVertex': labels(l)})
      return this
    },

    outVertex: function(l) {
      query.push({'outVertex': labels(l)})
      return this
    },

    identity: function() {
      query.push({'identity': true})
      return this
    },

    mark: function(l) {
      query.push({'as': labels(l)})
      return this
    },

    select: function(l) {
      query.push({'select': labels(l)})
      return this
    },

    by: function(key) {
      // a key is either a string or an inner query that has already been
      // built and passed in.
      query.push({'by': _.isString(key) ? {'key': key} : {'query': key.query}})
      return this
    },

    id: function() {
      query.push({'id': true})
      return this
    },

    label: function() {
      query.push({'label': true})
      return this
    },

    values: function(l) {
      query.push({'values': labels(l)})
      return this
    },

    properties: function(l) {
      query.push({'properties': labels(l)})
      return this
    },

    propertyMap: function(l) {
      query.push({'propertyMap': labels(l)})
      return this
    },

    dedup: function(l) {
      query.push({'dedup': labels(l)})
      return this
    },

    limit: function(l) {
      query.push({'limit': l})
      return this
    },

    range: function(lower, upper) {
      query.push({'lower': lower, 'upper': upper})
      return this
    },

    count: function() {
      query.push({'count': true})
      return this
    },

    path: function() {
      query.push({'path': true})
      return this
    },

    aggregate: function(label) {
      query.push({'aggregate': label})
      return this
    },

    // group: function(by) {
    //   if (!)
    //   query.push({'groupCount': label})
    //   return this
    // },

    groupCount: function(b) {
      query.push({'groupCount': by(b)})
      return this
    },

    is: function(condition) {
      query.push({'is': condition})
      return this
    },

    has: function(key, h) {
      var out = {'key': key}
      if (_.isString(h) || _.isNumber(h)) {
        out['value'] = value(h)
      } else if (_.isArray(h)) {
        out['query'] = h.query
      } else {
        out['condition'] = h
      }

      query.push({'has': out})
      return this
    },

    hasLabel: function(l) {
      query.push({'hasLabel': labels(l)})
      return this
    },    

    hasNot: function(key) {
      query.push({'hasNot': key})
      return this
    },    

    match: function(queries) {
      query.push({'match': {'queries': _.map(function(query) {return query.query}, queries)}})
      return this
    },

    execute: function(callback) {
      parent.execute({query:query}, callback)
    }
  }

  return operations
}

function Ophion() {
  var queryBase = '/vertex/query'

  function value(x) {
    if (_.isString(x)) {
      v = {'s': x}
    } else if (_.isNumber(x)) {
      if (x === Math.floor(x)) {
        v = {'n': x}
      } else {
        v = {'r': x}
      }
    }

    return v ? v : x
  }

  return {
    execute: function(query, callback) {
      fetch(queryBase, {
        method: 'POST',
        headers: {'Content-Type': 'application/json', 'Accept': 'application/json'},
        body: JSON.stringify(query),
      }).then(function(response) {
        return response.json()
      }).then(callback)
    },

    query: function() {
      return OphionQuery(this)
    },

    q: function() {
      return OphionQuery(this)
    },

    as: function(l) {
      return OphionQuery(this).as(l)
    },

    eq: function(x) {
      return {'eq': value(x)}
    },

    neq: function(x) {
      return {'neq': value(x)}
    },

    gt: function(x) {
      return {'gt': value(x)}
    },

    gte: function(x) {
      return {'gte': value(x)}
    },

    lt: function(x) {
      return {'lt': value(x)}
    },

    lte: function(x) {
      return {'lte': value(x)}
    },

    between: function(lower, upper) {
      return {'between': {'lower': value(lower), 'upper': value(upper)}}
    },

    inside: function(lower, upper) {
      return {'inside': {'lower': value(lower), 'upper': value(upper)}}
    },

    outside: function(lower, upper) {
      return {'outside': {'lower': value(lower), 'upper': value(upper)}}
    },

    within: function(v) {
      return {'within': {'values': _.map(value, v)}}
    },

    without: function(v) {
      return {'without': {'values': _.map(value, v)}}
    }
  }  
}

module.exports = {
  Ophion: Ophion,
  callsync: callsync
}
