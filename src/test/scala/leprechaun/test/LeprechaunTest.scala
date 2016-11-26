package leprechaun.test

import leprechaun._
import shapeless._
import gremlin.scala._
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory
import org.scalatest._

class LeprechaunTest extends FunSuite {
  def graph = TinkerFactory.createModern.asScala

  test("construction") {
    val operations = OutOperation("created") :: VertexOperation("person") :: HNil
    val result = Operation.process(operations, graph).toList
    println(result)
    assert(result.size == 4)
  }

  test("has") {
    val operations = HasOperation("name", List("marko", "vadas")) :: InOperation("created") :: VertexOperation("software") :: HNil
    val result = Operation.process(operations, graph).toList
    println(result)
    assert(result.size == 1)
  }

  test("as") {
    val operations = AsOperation("yellow") :: HasOperation("name", List("ripple", "lop")) :: OutOperation("created") :: VertexOperation("person") :: HNil
    val result = Operation.process(operations, graph).toList
    println(result)
    assert(result.size == 4)
  }

  test("operation") {
    val raw = """{"query": [{"vertex": "person"},{"has": "name", "within": ["marko", "vadas"]},{"out": "created"}]}"""
    val query = Query.fromString(raw)
    val result: GremlinScala[Vertex, HList] = query.operate(graph)
    println(result)
    assert(result.toList.size == 1)
  }

  // trying to get hlist to work
  // -------------------------

  // test("hlist") {
  //   val raw = """{"query": [{"vertex": "person"},{"has": "name", "within": ["marko", "vadas"]},{"out": "created"}]}"""
  //   val query = Query.fromString(raw)
  //   val operations = Generic[Query].to(query)
  //   val result = Operation.process(operations, graph).toList
  //   println(result)
  //   assert(result.toList.size == 1)
  // }
}
