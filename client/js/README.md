## About

A js client for making [Ophion](https://github.com/bmeg/ophion) queries.


## Install

```
npm install -s ophion
```


## Usage

### Ophion


A test program

```
#!/usr/bin/env node
'use strict';
var Ophion = require('ophion').Ophion;
var O = Ophion('http://bmeg.compbio.ohsu.edu');
O.query().limit(1).execute(function(rsp){console.log(rsp)});
```

Should print

```
[ { label: 'Variant',
    gid: 'variant:5:70936890:70936890:A:A,G',
    properties:
     { id: 'variant:5:70936890:70936890:A:A,G',
       start: '70936890',
       end: '70936890',
       referenceName: '5',
       alternateBases: 'A,G',
       referenceBases: 'A',
       'info.center': '["SOMATICSNIPER","RADIA","MUTECT","MUSE","VARSCANS"]' } } ]
```       



### Callsync

A test program

```
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
```

Should print

```
{ initial: 21, yellow: 26, green: 2, outcome: 2 }
```



For more, [see here.](https://github.com/bmeg/ophion/blob/master/client/python/README.md)




## Contribute

```
# cd to this directory
npm publish

# visit https://www.npmjs.com/package/ophion
```
