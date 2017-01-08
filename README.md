# ophion

Language for making graph queries from data

![OPHION](https://github.com/bmeg/ophion/blob/master/resources/ophion.jpg)

## Usage

Given a graph adhering to the [tinkerpop](https://tinkerpop.apache.org/) interface, we want to make remote queries against it without exposing a gremlin console. To this end, **Ophion** provides a json-encoded schema for making graph queries as data:

```json
{"query":
 [{"label": "person"},
  {"as": "people"},
  {"outEdge": "created"},
  {"has": "weight", "within": [1.0]},
  {"inVertex": "software"},
  {"as": "software"},
  {"select": ["people", "software"]}]}
```

Given some json like this, you can parse it into a program that executes the given query when run:

```scala
import ophion.Ophion._
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory

val graph = TinkerFactory.createModern
val traversal = graph.traversal.V()

val example = """{"query":
 [{"label": "person"},
  {"as": "people"},
  {"outEdge": "created"},
  {"has": "weight", "within": [1.0]},
  {"inVertex": "software"},
  {"as": "software"},
  {"select": ["people", "software"]}]}"""

val query = Query.fromString(example).compose
val result = query.foldMap(operationInterpreter(traversal)).head.toList
```

## Gremlin functionality currently supported:

### selecting vertexes or edges by label

    {"label": "alphabet"}

### values - getting just certain values rather than the whole vertex or edge property map

    {"values": ["color", "pattern", "hair", "volume"]}

### limit and range

    {"limit": 13}
    {"begin": 84, "end": 87}

### `has...within` predicates

    {"has": "arrow", "within": ["feathered", "of time", "prehistoric", "guide"]}

### traversing from and to edges and vertexes

    {"in": "pointing at me"}
    {"out": "who I'm pointing at"}
    {"inEdge": "hop up to the edge"}
    {"outEdge": "same but with outgoing"}
    {"inVertex": "from an edge, go to the vertex at one end"}
    {"outVertex": "go to the vertex on the other end"}

### count and counting by a property

    {"count": ""}
    ..., {"groupCount": ""}, {"by": "color"}, ...

You can label different group counts and retrieve them with `cap`:

    ..., 
    {"groupCount": "color"},
    {"by": "color"},
    {"groupCount": "faces"},
    {"by": "faces"},
    {"cap", ["color", "faces"]}, ...

### marking a point in a traversal with `as` and `select`ing those points at the end of the traversal.

    {"as": "birth place"}
    ... (do some stuff)
    {"as": "resting place"}
    {"select": ["birth place", "resting place"]} --> just get the endpoints

**Ophion** hopes to support the entire gremlin spec in the future, but for now these features already enable all of our current graph query use cases.