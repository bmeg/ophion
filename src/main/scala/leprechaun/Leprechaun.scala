package leprechaun

import shapeless._
import shapeless.ops.hlist.RightFolder
import shapeless.ops.hlist.Prepend

import gremlin.scala._
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.json4s._
import org.json4s.jackson._
import org.json4s.jackson.JsonMethods._

sealed trait Operation

case class VertexOperation(vertex: String) extends Operation {
  def operate(input: ScalaGraph): GremlinScala[Vertex, HNil] = {
    input.V.hasLabel(vertex)
  }
}

case class HasOperation[M](has: String, within: List[M]) extends Operation {
  def operate[Labels <: HList](input: GremlinScala[Vertex, Labels]): GremlinScala[Vertex, Labels] = {
    val gid = Key[M](has)
    input.has(gid, P.within(within:_*))
  }
}

case class AsOperation(as: String) extends Operation {
  def operate[A, In <: HList](input: GremlinScala[A, In]) (implicit p: Prepend[In, ::[A, HNil]]): GremlinScala[A, p.Out] = {
    input.as(as)
  }
}

case class InOperation(in: String) extends Operation {
  def operate[Labels <: HList](input: GremlinScala[Vertex, Labels]): GremlinScala[Vertex, Labels] = {
    input.in(in)
  }
}

case class OutOperation(out: String) extends Operation {
  def operate[Labels <: HList](input: GremlinScala[Vertex, Labels]): GremlinScala[Vertex, Labels] = {
    input.out(out)
  }
}

case class InEdgeOperation(inEdge: String) extends Operation {
  def operate[Labels <: HList](input: GremlinScala[Vertex, Labels]): GremlinScala[Edge, Labels] = {
    input.inE(inEdge)
  }
}

case class OutEdgeOperation(outEdge: String) extends Operation {
  def operate[Labels <: HList](input: GremlinScala[Vertex, Labels]): GremlinScala[Edge, Labels] = {
    input.outE(outEdge)
  }
}

case class InVertexOperation(inVertex: String) extends Operation {
  def operate[Labels <: HList](input: GremlinScala[Edge, Labels]): GremlinScala[Vertex, Labels] = {
    input.inV()
  }
}

case class OutVertexOperation(outVertex: String) extends Operation {
  def operate[Labels <: HList](input: GremlinScala[Edge, Labels]): GremlinScala[Vertex, Labels] = {
    input.outV()
  }
}

object Query {
  class OperationSerializer extends CustomSerializer[Operation](format => ({
    case JObject(List(JField("vertex", JString(vertex)))) => VertexOperation(vertex)
    case JObject(List(JField("has", JString(has)), JField("within", within))) => HasOperation(has, within.extract[List[String]])
    case JObject(List(JField("as", JString(as)))) => AsOperation(as)
    case JObject(List(JField("in", JString(in)))) => InOperation(in)
    case JObject(List(JField("out", JString(out)))) => OutOperation(out)
    case JObject(List(JField("inVertex", JString(inVertex)))) => InVertexOperation(inVertex)
    case JObject(List(JField("outVertex", JString(outVertex)))) => OutVertexOperation(outVertex)
    case JObject(List(JField("inEdge", JString(inEdge)))) => InEdgeOperation(inEdge)
    case JObject(List(JField("outEdge", JString(outEdge)))) => OutEdgeOperation(outEdge)
  }, {
    case VertexOperation(vertex) => JObject(JField("vertex", JString(vertex)))
    case InOperation(in) => JObject(JField("in", JString(in)))
  }))

  implicit val formats = Serialization.formats(NoTypeHints) + new OperationSerializer()

  def fromJson(json: JValue): Query = {
    json.extract[Query]
  }

  def fromString(raw: String): Query = {
    val json = parse(raw)
    fromJson(json)
  }
}

case class Query(query: List[Operation]) {
  // def operate[R, In <: HList, G](graph: ScalaGraph) (implicit p: Prepend[In, ::[G, HNil]]): R = {
  def operate[R](graph: ScalaGraph): R = {
    var anvil: Any = graph
    def op[M](operation: Operation) {
      operation match {
        case VertexOperation(vertex) => anvil = operation.asInstanceOf[VertexOperation].operate(anvil.asInstanceOf[ScalaGraph])
        case HasOperation(has, within: List[M]) => anvil = {
          val gid = Key[M](has)
          operation.asInstanceOf[HasOperation[M]].operate(anvil.asInstanceOf[GremlinScala[Vertex, HList]].has(gid, P.within(within:_*)))
        }
        // case AsOperation(as) => anvil = operation.asInstanceOf[AsOperation].operate(anvil.asInstanceOf[GremlinScala[M, HList]])
        case InOperation(in) => anvil = operation.asInstanceOf[InOperation].operate(anvil.asInstanceOf[GremlinScala[Vertex, HList]])
        case OutOperation(out) => anvil = operation.asInstanceOf[OutOperation].operate(anvil.asInstanceOf[GremlinScala[Vertex, HList]])
        case InVertexOperation(inVertex) => anvil = operation.asInstanceOf[InVertexOperation].operate(anvil.asInstanceOf[GremlinScala[Edge, HList]])
        case OutVertexOperation(outVertex) => anvil = operation.asInstanceOf[OutVertexOperation].operate(anvil.asInstanceOf[GremlinScala[Edge, HList]])
        case InEdgeOperation(inEdge) => anvil = operation.asInstanceOf[InEdgeOperation].operate(anvil.asInstanceOf[GremlinScala[Vertex, HList]])
        case OutEdgeOperation(outEdge) => anvil = operation.asInstanceOf[OutEdgeOperation].operate(anvil.asInstanceOf[GremlinScala[Vertex, HList]])
      }
    }

    query.foreach(x => op(x))
    anvil.asInstanceOf[R]
  }
}

trait ApplyOperationDefault extends Poly2 {
  implicit def default[T, L <: HList] = at[T, L] ((_, acc) => acc)
}

object ApplyOperation extends ApplyOperationDefault {
  implicit def vertex[T, L <: HList, S <: HList] = at[VertexOperation, ScalaGraph] ((t, acc) => t.operate(acc))
  implicit def has[M, T, L <: HList, S <: HList] = at[HasOperation[M], GremlinScala[Vertex, S]] ((t, acc) => t.operate(acc))
  implicit def as[A, T, L <: HList, In <: HList](implicit p: Prepend[In, ::[A, HNil]]) = at[AsOperation, GremlinScala[A, In]] ((t, acc) => t.operate(acc))
  implicit def in[T, L <: HList, S <: HList] = at[InOperation, GremlinScala[Vertex, S]] ((t, acc) => t.operate(acc))
  implicit def out[T, L <: HList, S <: HList] = at[OutOperation, GremlinScala[Vertex, S]] ((t, acc) => t.operate(acc))
  implicit def inEdge[T, L <: HList, S <: HList] = at[InEdgeOperation, GremlinScala[Vertex, S]] ((t, acc) => t.operate(acc))
  implicit def outEdge[T, L <: HList, S <: HList] = at[OutEdgeOperation, GremlinScala[Vertex, S]] ((t, acc) => t.operate(acc))
  implicit def inVertex[T, L <: HList, S <: HList] = at[InVertexOperation, GremlinScala[Edge, S]] ((t, acc) => t.operate(acc))
  implicit def outVertex[T, L <: HList, S <: HList] = at[OutVertexOperation, GremlinScala[Edge, S]] ((t, acc) => t.operate(acc))
}

object Operation {
  def process[Input, Output, A <: HList](operations: A, input: Input) (implicit folder: RightFolder.Aux[A, Input, ApplyOperation.type, Output]): Output = {
    operations.foldRight(input) (ApplyOperation)
  }
}

