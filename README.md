# ophion

Language for making graph queries from data

![OPHION](https://github.com/bmeg/ophion/blob/master/resources/public/img/ophion.png)

# usage

Ophion queries are an array of graph operations. Each element of this array is itself an array, with the first element being the name of the operation and the rest of the elements parameters to this operation.

### example

Given this schema (this is the current BMEG schema):

![SCHEMA](https://github.com/bmeg/ophion/blob/master/resources/public/bmeg-schema-march-2018.png)

Here is an example query that starts from Individual vertexes from BRCA and traverses to all associated Variants called from MUTECT, and returns every Individual/Variant pair:

    [["where",{"disease_code":"BRCA"}],
     ["mark","individual"],
     ["from","sampleOf","Biosample"],
     ["from","callSetOf","CallSet"],
     ["where",{"method":"MUTECT"}],
     ["fromUnique","variantCall","Variant"],
     ["mark","variant"],
     ["select",["individual","variant"]]]

This is the actual json payload. You can deliver this through any http method you wish to use. Here is an example query through curl that gets all Biosamples for a given Individual:

    curl \
    -X POST \
    -H "Content-Type: application/json" \
    -d '[["mark","individual"],["from","sampleOf","Biosample"],["mark","biosample"],["select",["individual","biosample"]]]' \
    http://10.96.11.144/query/Individual

If you notice, the starting vertex label is in the url, while the query is posted as json.

# operations

The operations currently supported are:

### traversals

Traversal operations help you navigate the graph:

* from
* to
* fromEdge
* toEdge
* fromVertex
* toVertex
* fromUnique
* toUnique

### state

Mark and select work together to keep track of state during traversals:

* mark
* select

### filters

Filter operations help you cull or craft the results:

* where
* dedup
* limit
* order
* offset
* match
* values

### aggregates

Aggregate operations assemble metrics from a set of results:

* count
* gather
* aggregate
* groupCount

Each of these operations is explained in more detail below.

# traversals

The two main traversal operations are `from` and `to`. Because Ophion works with directed graphs, there is a distinction between traversing across an edge in either direction. Either requires two additional pieces of information, an edge label and the ultimate destination label.

One upshot of this is that in order to craft traversals, you need to know the schema of the graph you are querying. The raw schema is defined in a [protograph](https://github.com/bmeg/protograph) file (for BMEG the [protograph file is here](https://github.com/biostream/bmeg-etl/blob/master/bmeg.protograph.yaml)). This will show you all vertex labels and all edges they have to other labels.

If you want a visual representation then you can use the [BMEG website code](https://github.com/bmeg/bmeg) to generate a schema in cytoscape like the example above.

Either way, once you have your schema you know a few key things:

* vertex labels
* edge labels
* edge directions

These are the things you need to know to successfully traverse the graph.

### to

The `to` operation travels across outgoing edges. It requires two pieces of information: the edge label, and the destination label:

    ["to", $edgeLabel, $destinationLabel]

As an example say we are on a Variant vertex and want to see what Gene the Variant is in. The query would look like this:

    ["to", "variantIn", "Gene"]

### from

The `from` operation is the same as `to` but going in the other direction. To demonstrate this, let's reverse the previous query and find every Variant that is in a given Gene (assuming we are already on a Gene vertex):

    ["from", "variantIn", "Variant"]

### toUnique/fromUnique

Say you are going from Variants to Genes. Every Gene has many associated Variants, so if you go this direction you will end up with a lot of the same Genes in your traversal. Sometimes this is what you want, but sometimes you don't care where they came from, you just want to know which Genes were implicated in your given set of Variants.

The `toUnique` and `fromUnique` operations work exactly the same as their non-unique counterparts, but the outcome will merge all identical vertexes.

### toEdge/fromEdge, toVertex/fromVertex

The *Edge and *Vertex traversals are for navigating to and from the edge records between vertexes. Here is an example of going step by step between Gene and Variant (starting on Gene):

    [["fromEdge", "variantIn"],
     ....
     (currently on the edge)
     ....
     ["fromVertex", "Variant"]]

This might seem like just a more verbose way to get from Genes to Variants, but sometimes interesting information is stored on edges that you could filter on. And edges themselves may contain the information you are looking for, in which case you don't even need to continue the traversal.

As you may have guessed, `to` and `from` are just shorthand for the sequence `toEdge/toVertex` or `fromEdge/fromVertex`.

# state

As you traverse through the graph sometimes you want to keep track of where you are. Also, often you care about what things are associated to what other things you were visiting previously. This is what `mark` and `select` are for.

### mark/select

To save the current point of your traversal you can call `mark` with a label, and at the end of the traversal you can `select` any of the `mark`s you made along the way:

    [["mark","individual"],
     ["from","sampleOf","Biosample"],
     ["mark","biosample"],
     ["select",["individual","biosample"]]]

This traversal starts at Individual, marks it, then traverses to Biosample, marks that as well, then finally `select`s the two marked elements. This will return objects with the keys from the `select`, whose values are the gids of the marked elements.

    ....
    {"individual": "individual:TCGA-XE-ANJJ", "biosample": "biosample:tcga:TCGA-XE-ANJJ:normal"}
    {"individual": "individual:TCGA-XE-ANJJ", "biosample": "biosample:tcga:TCGA-XE-ANJJ:tumor"}
    ....

# filters

Often you are making a query to find specific things, not just all vertexes or edges of one type. Filters allow you to craft what elements your traversal is concerned with.

### where

The `where` operation is the way to select specific elements during a traversal. For instance, if I start a query from Individual it will happily return all Individuals, but what if I am only concerned with Individuals with breast cancer? This is where `where` comes in:

    ["where": {"disease_code": "BRCA"}]

This will work with any property in the vertex:

    ["where": {"gender": "MALE"}]

and can even work with intersections (AND):

    ["where": {"disease_code": "BRCA", "gender": "MALE"}] // probably zero results

The `where` syntax is based off of [mongo conditionals](https://docs.mongodb.com/manual/tutorial/query-documents/#read-operations-query-argument), so anything they can do you can do. Here are a couple of other useful examples.

Say you have a list of values and want to see if any of them match a given field. You can use `$in`:

    ["where": {"disease_code": {"$in": ["BRCA", "SKCM", "LUAD"]}}]

We can have multiple keys to do an AND, what about an OR?

    ["where": {"$or" [{"disease_code": "BRCA"}, {"gender": "MALE"}]}] // more results this time

That covers most of the use cases, but there are many more.

### dedup

The `dedup` operator will cull duplicate elements. This rarely needs to be called on its own since we have the `toUnique` and `fromUnique` operators which combine it with a traversal (when it is most efficient to do so), but is provided for the cases when it still makes sense to invoke directly.

`dedup` can be called on its own with no arguments (in which case it deduplicates based on gid), or you can supply the field. For instance, if you ran

    ["dedup", "gender"]

on all Individuals you would be left with one record with MALE as gender and one with FEMALE.

### limit/offset

There are a large number of records in the db, and often times you don't need to return all of them. Use `limit` to limit the number of records returned:

    ["limit", 10]

This will return only the first 10. When combined with `offset` and `order` you can emulate pagination.

`offset` will drop a given number of records and return the rest. This would get all of the records the `limit` above did not return:

    ["offset", 10]

### order

The default ordering in the database is when the document was inserted (timestamp). If you want them to come back in a different order you can use the `order` operation:

    ["order", {}]

* match
* values
