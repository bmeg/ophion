package ophion.test

import cats.free.Free

import org.apache.tinkerpop.gremlin.structure.{Vertex, Edge}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory

import org.scalatest._

import ophion.Ophion._
import ophion.Ophion.Operation._

import scala.collection.JavaConversions._

class OphionTest extends FunSuite {
  def graph = TinkerFactory.createModern

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
}
