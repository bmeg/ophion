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
    assert(result.size == 4)
  }

  test("has") {
    val operations = HasOperation("name", List("marko", "vadas")) :: InOperation("created") :: VertexOperation("software") :: HNil
    val result = Operation.process(operations, graph).toList
    assert(result.size == 1)
  }

  test("as") {
    val operations = AsOperation("yellow") :: HasOperation("name", List("peter", "lop")) :: InOperation("created") :: VertexOperation("person") :: HNil
    val result = Operation.process(operations, graph).toList
    assert(result.size == 2)
  }

  test("operation") {
    val raw = """{"query": [{"vertex": "person"},{"out": "created"}]}"""
    val query = Query.fromString(raw)
    val result: GremlinScala[Vertex, HList] = query.operate(graph)
    assert(result.toList.size == 4)
  }
}
