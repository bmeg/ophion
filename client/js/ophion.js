var _ = require('underscore');


require('es6-promise').polyfill();
require('isomorphic-fetch');
var callsync = require('./callsync')

function value(x) {
  var v = null
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


function OphionQuery(parent) {
  var parent = parent

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
      return {'key': b}
    } else {
      return {'query': b.query}
    }
  }

  return {
    query: [],

    V: function(l) {
      this.query.push({'V': labels(l)})
      return this
    },

    E: function(l) {
      this.query.push({'E': labels(l)})
      return this
    },

    searchVertex: function(term, search) {
      if (search) {
        this.query.push({'searchVertex': {'term': term, 'search': search}})
      } else {
        this.query.push({'searchVertex': {'search': term}})
      }

      return this
    },

    incoming: function(l) {
      this.query.push({'in': labels(l)})
      return this
    },

    outgoing: function(l) {
      this.query.push({'out': labels(l)})
      return this
    },

    inEdge: function(l) {
      this.query.push({'inEdge': labels(l)})
      return this
    },

    outEdge: function(l) {
      this.query.push({'outEdge': labels(l)})
      return this
    },

    inVertex: function(l) {
      this.query.push({'inVertex': labels(l)})
      return this
    },

    outVertex: function(l) {
      this.query.push({'outVertex': labels(l)})
      return this
    },

    identity: function() {
      this.query.push({'identity': true})
      return this
    },

    mark: function(l) {
      this.query.push({'as': labels(l)})
      return this
    },

    select: function(l) {
      this.query.push({'select': labels(l)})
      return this
    },

    by: function(key) {
      // a key is either a string or an inner query that has already been
      // built and passed in.
      this.query.push({'by': _.isString(key) ? {'key': key} : {'query': key.query}})
      return this
    },

    id: function() {
      this.query.push({'id': true})
      return this
    },

    label: function() {
      this.query.push({'label': true})
      return this
    },

    values: function(l) {
      this.query.push({'values': labels(l)})
      return this
    },

    properties: function(l) {
      this.query.push({'properties': labels(l)})
      return this
    },

    propertyMap: function(l) {
      this.query.push({'propertyMap': labels(l)})
      return this
    },

    dedup: function(l) {
      this.query.push({'dedup': labels(l)})
      return this
    },

    limit: function(l) {
      this.query.push({'limit': l})
      return this
    },

    order: function(o, asc) {
      if (asc === undefined) {
        asc = true;
      }
      this.query.push({'order': {'key': o, 'ascending': asc}})
      return this
    },

    range: function(lower, upper) {
      this.query.push({'range': {'lower': lower, 'upper': upper}})
      return this
    },

    count: function() {
      this.query.push({'count': true})
      return this
    },

    path: function() {
      this.query.push({'path': true})
      return this
    },

    aggregate: function(label) {
      this.query.push({'aggregate': label})
      return this
    },

    // group: function(by) {
    //   if (!)
    //   query.push({'groupCount': label})
    //   return this
    // },

    groupCount: function(b) {
      this.query.push({'groupCount': by(b)})
      return this
    },

    is: function(condition) {
      this.query.push({'is': condition})
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

      this.query.push({'has': out})
      return this
    },

    hasLabel: function(l) {
      this.query.push({'hasLabel': labels(l)})
      return this
    },

    hasNot: function(key) {
      this.query.push({'hasNot': key})
      return this
    },

    match: function(queries) {
      this.query.push({'match': {'queries': _.map(queries, function(query) {return query.query})}})
      return this
    },

    execute: function(callback) {
      parent.execute(this.query, callback)
    },

    run: function() {
      return parent.run(this.query)
    }
  }
}

function parseJson(s) {
  try {
    return JSON.parse(s)
  } catch(err) {
    return s
  }
}

function Ophion(site='') {
  var queryBase = '/vertex/query'

  return {
    run: function(query) {
      return fetch(site + queryBase, {
        method: 'POST',
        headers: {'Content-Type': 'application/json', 'Accept': 'application/json'},
        body: JSON.stringify(query),
      }).then(function(response) {
        return response.text()
      }).then(function(text) {
        var lines = text.replace(/^\s+|\s+$/g, '').split("\n")
        return lines.map(parseJson)
      })
    },

    execute: function(query, callback) {
      fetch(site + queryBase, {
        method: 'POST',
        headers: {'Content-Type': 'application/json', 'Accept': 'application/json'},
        body: JSON.stringify(query),
      }).then(function(response) {
        return response.text()
      }).then(function(text) {
        var lines = text.replace(/^\s+|\s+$/g, '').split("\n")
        var parsed = lines.map(parseJson)
        callback(parsed)
      })
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
      return {'within': _.map(v, value)}
    },

    without: function(v) {
      return {'without': _.map(v, value)}
    },

    mark: function(l) {
      return OphionQuery(this).mark(l)
    }
  }
}

module.exports.Ophion = Ophion ;
module.exports.Callsync = callsync.callsync;
