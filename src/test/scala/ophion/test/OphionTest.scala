package ophion.test

import cats.free.Free

import org.apache.tinkerpop.gremlin.structure.{Vertex, Edge}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory

import org.json4s._
import org.json4s.jackson._
import org.json4s.jackson.JsonMethods._

import org.scalatest._

import ophion.Ophion._
import ophion.Ophion.Operation._

import scala.collection.JavaConversions._

class OphionTest extends FunSuite {
  def graph = TinkerFactory.createModern
  implicit val formats = Serialization.formats(NoTypeHints)

  def simpleQuery: Free[Operation, GraphTraversal[_, Vertex]] =
    for {
      a <- label("software")
      b <- in("created")
    } yield b

  def selectQuery: Free[Operation, GraphTraversal[_, _]] =
    for {
      _ <- label("person")
      _ <- as("people")
      _ <- outEdge("created")
      _ <- has("weight", List(1.0))
      _ <- inVertex("software")
      _ <- as("software")
      x <- select(List("people", "software"))
    } yield x

  val example = """{"query":
 [{"label": "person"},
  {"as": "people"},
  {"outEdge": "created"},
  {"has": "weight", "within": [1.0]},
  {"inVertex": "software"},
  {"as": "software"},
  {"select": ["people", "software"]}]}"""

  test("free monadic coproduct") {
    val traversal = graph.traversal.V()
    val result = simpleQuery.foldMap(operationInterpreter(traversal))
    val values = result.toList.map(el => el.values(el.keys.toList: _*))
    println(values)
    assert(values.size == 4)
  }

  test("as and select") {
    val traversal = graph.traversal.V()
    val result = selectQuery.foldMap(operationInterpreter(traversal))
    val values = result.toList
    println(values.head)
    assert(values.size == 1)
  }

  test("json parsing") {
    val traversal = graph.traversal.V()
    val query = Query.fromString(example)
    val result = query.compose.foldMap(operationInterpreter(traversal)).head.toList
    println(result)
    assert(query.query.size == 7)
    assert(result.size == 1)
  }

  test("interpretation") {
    val traversal = graph.traversal.V()
    val query = Query.fromString(example)
    val result = query.interpret(traversal).toList
    println(result)
    assert(query.query.size == 7)
    assert(result.size == 1)
  }

  test("running") {
    val query = Query.fromString(example)
    val result = query.run(graph)
    assert(result.size == 1)
  }

  test("conversion to vertex view") {
    val traversal = graph.traversal.V()
    val result = simpleQuery.foldMap(operationInterpreter(traversal)).toList.toList
    val json = Query.resultJson(result)
    println(compact(render(json)))
  }

  test("serializing a complex result") {
    val traversal = graph.traversal.V()
    val result = selectQuery.foldMap(operationInterpreter(traversal)).toList.toList
    val json = Query.resultJson(result)
    println(compact(render(json)))
  }

  test("string values") {
    val values = """{"query":
      [{"label": "person"},
       {"values": ["name"]}]}"""

    val query = Query.fromString(values)
    val result = query.run(graph)
    val json = Query.resultJson(result)
    println(compact(render(json)))
  }

  test("number values") {
    val values = """{"query":
      [{"label": "person"},
       {"values": ["age"]}]}"""

    val query = Query.fromString(values)
    val result = query.run(graph)
    val json = Query.resultJson(result)
    println(compact(render(json)))
  }

  test("limit") {
    val values = """{"query":
      [{"label": "person"},
       {"values": ["age"]},
       {"limit": 3}]}"""

    val query = Query.fromString(values)
    val result = query.run(graph)
    val json = Query.resultJson(result)
    assert(result.size == 3)
    println(compact(render(json)))
  }

  test("has property") {
    val values = """{"query": [{"has": "age"}]}"""
    val query = Query.fromString(values)
    val result = query.run(graph)
    val json = Query.resultJson(result)
    assert(result.size == 4)
    println(compact(render(json)))
  }

  test("has not property") {
    val values = """{"query": [{"hasNot": "age"}]}"""
    val query = Query.fromString(values)
    val result = query.run(graph)
    val json = Query.resultJson(result)
    assert(result.size == 2)
    println(compact(render(json)))
  }

  test("count") {
    val values = """{"query":
      [{"label": "person"},
       {"values": ["age"]},
       {"count": ""}]}"""

    val query = Query.fromString(values)
    val result = query.run(graph)
    val json = Query.resultJson(result)
    assert(result.get(0) == 4)
    println(compact(render(json)))
  }

  test("groupCount") {
    val values = """{"query":
      [{"label": "person"},
       {"groupCount": ""},
       {"by": "name"}]}"""

    val query = Query.fromString(values)
    val result = query.run(graph)
    val json = Query.resultJson(result)
    println("groupCount result - " + result.toString())
    val counts = result.head.asInstanceOf[java.util.HashMap[String, Long]].values.toSet
    assert(counts.size == 1)
    assert(counts.head == 1)
    println(compact(render(json)))
  }

  test("cap") {
    val values = """{"query":
      [{"label": "person"},
       {"groupCount": "a"},
       {"by": "name"},
       {"cap": ["a"]}]}"""

    val query = Query.fromString(values)
    val result = query.run(graph)
    val json = Query.resultJson(result)
    println("cap result - " + result.toString())
    val counts = result.head.asInstanceOf[java.util.HashMap[String, Long]].values.toSet
    assert(counts.size == 1)
    assert(counts.head == 1)
    println(compact(render(json)))
  }

  test("range") {
    val values = """{"query":
      [{"label": "person"},
       {"values": ["age"]},
       {"begin": 1, "end": 2}]}"""

    val query = Query.fromString(values)
    val result = query.run(graph)
    val json = Query.resultJson(result)
    assert(result.size == 1)
    println(compact(render(json)))
  }
}
