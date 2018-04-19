# ophion

Language for making graph queries from data

![OPHION](https://github.com/bmeg/ophion/blob/master/resources/public/img/ophion.png)

## usage

Ophion queries are an array of graph operations. Each element of this array is itself an array, with the first element being the name of the operation and the rest of the elements parameters to this operation.

The operations currently supported are:

#### traversals

Traversal operations help you navigate the graph:

* from
* to
* fromEdge
* toEdge
* fromVertex
* toVertex
* fromUnique
* toUnique

#### control

Mark and select work together to keep track of state during traversals:

* mark
* select

#### filters

Filter operations help you cull or craft the results:

* root
* dedup
* where
* match
* values
* limit
* order
* offset
* sort

#### aggregates

Aggregate operations assemble metrics from a set of results:

* count
* gather
* aggregate
* groupCount

