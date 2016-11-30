package leprechaun.test

// import shapeless.{::,HNil,HList}
import cats.free.Free

// import gremlin.scala._
import org.apache.tinkerpop.gremlin.structure.{Vertex, Edge}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory

import org.scalatest._

import leprechaun.OperationCoproduct._
import leprechaun.OperationCoproduct.Operation._

import scala.collection.JavaConversions._

class LeprechaunTest extends FunSuite {
  def graph = TinkerFactory.createModern
  def code = List()

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

  }

  // test("construction") {
  //   val operations = OutOperation("created") :: VertexOperation("person") :: HNil
  //   val result = Operation.process(operations, graph).toList
  //   println(result)
  //   assert(result.size == 4)
  // }

  // test("has") {
  //   val operations = HasOperation("name", List("marko", "vadas")) :: InOperation("created") :: VertexOperation("software") :: HNil
  //   val result = Operation.process(operations, graph).toList
  //   println(result)
  //   assert(result.size == 1)
  // }

  // test("as") {
  //   val operations = AsOperation("yellow") :: HasOperation("name", List("ripple", "lop")) :: OutOperation("created") :: VertexOperation("person") :: HNil
  //   val result = Operation.process(operations, graph).toList
  //   println(result)
  //   assert(result.size == 4)
  // }

  // test("operation") {
  //   val raw = """{"query": [{"vertex": "person"},{"has": "name", "within": ["marko", "vadas"]},{"out": "created"}]}"""
  //   val query = Query.fromString(raw)
  //   val result: GremlinScala[Vertex, HList] = query.operate(graph)
  //   println(result)
  //   assert(result.toList.size == 1)
  // }
}
