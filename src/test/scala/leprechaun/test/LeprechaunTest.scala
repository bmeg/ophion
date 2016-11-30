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
  def program: Free[Operation, GraphTraversal[_, Vertex]] =
    for {
      a <- label("software")
      b <- in("created")
    } yield b

  test("free monadic coproduct") {
    val traversal = graph.traversal.V()
    val result = program.foldMap(operationInterpreter(traversal))
    val values = result.toList.map(el => el.values(el.keys.toList: _*))
    println(program)
    println(program.getClass)
    println(values)
    assert(values.size == 4)
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
