// callsync.js

// Pass in a catalog, which is a map of keys to functions, and it will return a function
// that when called, executes a given path through the catalog on a given initial value.

// Functions in the catalog take two arguments. The first is the value to operate on,
// and the second is a callback to send the outcome to (instead of returning a value).

require('underscore')

function callsync(catalog, outputName) {
  if (!outputName) {
    outputName = 'outcome';
  }

  function execute(path, value) {
    var flat = _.flatten(path);
    var here = _.first(flat);
    var along = _.rest(flat);
    var step = catalog[here];

    if (step) {
      return step(value, function(outcome) {
        value[here] = outcome;
        if (along.length != 0) {
          return execute(along, value);
        } else {
          value[outputName] = outcome;
          return value;
        }
      });
    } else {
      console.log("no step by the name " + here);
      return execute(along, value);
    }
  }

  return execute;
}

module.exports = {
  callsync: callsync
}

// Example:

// var series = callsync({
//   yellow: function(value, callback) {
//     return callback(value['initial'] + 5)
//   },

//   green: function(value, callback) {
//     return callback(value['yellow'] / 13)
//   }
// });

// series(['yellow', 'green'], {initial: 21});

// -----> {initial: 21, yellow: 26, green: 2, outcome: 2}
