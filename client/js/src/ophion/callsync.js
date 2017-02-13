// callsync.js

// Pass in a catalog, which is a map of keys to functions, and it will return a function
// that when called, executes a given path through the catalog on a given initial value.

// Functions in the catalog take two arguments. The first is the value to operate on,
// and the second is a callback to send the outcome to (instead of returning a value).

import * as _ from 'underscore';

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

export {
  callsync
}
