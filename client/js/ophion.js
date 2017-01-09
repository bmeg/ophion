

function OphionQuery(parent) {
  var parent = parent;
  var query = [];
  
  return {
    label: function(label) {
       query.push({'label': label});
       return this;
    },    
    has: function(prop, within) {
       query.push({'has': prop, 'within': within});
       return this;
    },
    values: function(v) {
       query.push({'values': v})
       return this
    },
    incoming: function(label) {
       query.push({'in': label})
       return this
    },
    outgoing: function(label) {
       query.push({'out': label})
       return this
    },
    inEdge: function(label) {
       query.push({'inEdge': label})
       return this
    },
    outEdge: function(label) {
       query.push({'outEdge': label})
       return this
    },
    inVertex: function(label) {
       query.push({'inVertex': label})
       return this
    },
    outVertex: function(label) {
       query.push({'outVertex': label})
       return this
    },
    mark: function(label) {
       query.push({'as': label})
       return this
    },
    limit: function(l) {
       query.push({'limit': l})
       return this
    },
    range: function(begin, end) {
       query.push({'begin': begin, 'end': end})
       return this
    },
    count: function() {
       query.push({'count': ''})
       return this
    },
    groupCount: function(label) {
        query.push({'groupCount': label})
        return this
    },
    by: function(label) {
        query.push({'by': label})
        return this
    },
    cap: function(c) {
        query.push({'cap': c})
        return this
    },
    execute: function(callback) {
        parent.execute({query:query}, callback)
    }
  };  
}

function Ophion() {
  
  var out = {
      execute: function(query, callback) {
        $.ajax({
          url: "/gaia/vertex/query",
          dataType: 'json',
          type: 'POST',
          data: JSON.stringify(query),
          success: function(results) {
            callback(results)
          }
        })        
      },
      query: function() {
        return OphionQuery(this)
      }
  }  
  return out  
}
