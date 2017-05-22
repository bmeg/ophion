#!/usr/bin/env node
'use strict';

var Ophion = require('ophion').Ophion;
var O = Ophion('http://bmeg.compbio.ohsu.edu');
O.query().limit(1).execute(function(x){console.log(x)});


var callsync = require('ophion').Callsync;
var series = callsync({
  yellow: function(value, callback) {
    return callback(value['initial'] + 5)
  },

  green: function(value, callback) {
    return callback(value['yellow'] / 13)
  }
});

console.log(series(['yellow', 'green'], {initial: 21}));
