package ophion

import cats._
import cats.implicits._
import cats.free.Free

import org.apache.tinkerpop.gremlin.structure.{Graph, Element, Property, Vertex, Edge, VertexProperty}
import org.apache.tinkerpop.gremlin.process.traversal.{P, Traversal, Traverser}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal

import org.json4s._
import org.json4s.jackson._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}

import scala.collection.JavaConversions._
import java.util.function.{Predicate ⇒ JPredicate, Consumer ⇒ JConsumer, BiFunction ⇒ JBiFunction, Function => JFunction}

object Ophion {
  type FreeOperation[F] = Free[Operation, F]

  sealed trait Operation[O]
  case object IdentityOperation extends Operation[GraphTraversal[_, _]]
  case class LabelOperation(label: String) extends Operation[GraphTraversal[_, Vertex]]
  case class HasOperation(has: String, within: List[_]) extends Operation[GraphTraversal[_, Vertex]]
  case class HasNotOperation(hasNot: String) extends Operation[GraphTraversal[_, Vertex]]
  case class ValuesOperation(values: List[String]) extends Operation[GraphTraversal[_, _]]
  case class CapOperation(cap: List[String]) extends Operation[GraphTraversal[_, _]]
  case class InOperation(in: String) extends Operation[GraphTraversal[_, Vertex]]
  case class OutOperation(out: String) extends Operation[GraphTraversal[_, Vertex]]
  case class InEdgeOperation(inEdge: String) extends Operation[GraphTraversal[_, Edge]]
  case class OutEdgeOperation(outEdge: String) extends Operation[GraphTraversal[_, Edge]]
  case class InVertexOperation(inVertex: String) extends Operation[GraphTraversal[_, Vertex]]
  case class OutVertexOperation(outVertex: String) extends Operation[GraphTraversal[_, Vertex]]
  case class AsOperation(as: String) extends Operation[GraphTraversal[_, _]]
  case class GroupCountOperation(groupCount: String) extends Operation[GraphTraversal[_, _]]
  case class ByOperation(by: String) extends Operation[GraphTraversal[_, _]]
  case class BackOperation(back: String) extends Operation[GraphTraversal[_, _]]
  case class PropGreaterOperation(prop: String, gt: Double) extends Operation[GraphTraversal[_, _]]
  case class PropLessOperation(prop: String, lt: Double) extends Operation[GraphTraversal[_, _]]
  case class PropEqualsOperation(prop: String, eq: Double) extends Operation[GraphTraversal[_, _]]
  case class LimitOperation(limit: Long) extends Operation[GraphTraversal[_, _]]
  case class RangeOperation(begin: Long, end: Long) extends Operation[GraphTraversal[_, _]]
  case class SelectOperation(select: List[String]) extends Operation[GraphTraversal[_, _]]
  case class CountOperation(count: String) extends Operation[GraphTraversal[_, Long]]
  case class DedupOperation(dedup: String) extends Operation[GraphTraversal[_, _]]

  object Operation {
    def identity: FreeOperation[GraphTraversal[_, _]] = Free.liftF(IdentityOperation)
    def label(v: String): FreeOperation[GraphTraversal[_, Vertex]] = Free.liftF(LabelOperation(v))
    def has(h: String, w: List[_]): FreeOperation[GraphTraversal[_, Vertex]] = Free.liftF(HasOperation(h, w))
    def hasNot(h: String): FreeOperation[GraphTraversal[_, Vertex]] = Free.liftF(HasNotOperation(h))
    def values(v: List[String]): FreeOperation[GraphTraversal[_, _]] = Free.liftF(ValuesOperation(v))
    def cap(c: List[String]): FreeOperation[GraphTraversal[_, _]] = Free.liftF(CapOperation(c))
    def in(i: String): FreeOperation[GraphTraversal[_, Vertex]] = Free.liftF(InOperation(i))
    def out(o: String): FreeOperation[GraphTraversal[_, Vertex]] = Free.liftF(OutOperation(o))
    def inEdge(ie: String): FreeOperation[GraphTraversal[_, Edge]] = Free.liftF(InEdgeOperation(ie))
    def outEdge(oe: String): FreeOperation[GraphTraversal[_, Edge]] = Free.liftF(OutEdgeOperation(oe))
    def inVertex(iv: String): FreeOperation[GraphTraversal[_, Vertex]] = Free.liftF(InVertexOperation(iv))
    def outVertex(ov: String): FreeOperation[GraphTraversal[_, Vertex]] = Free.liftF(OutVertexOperation(ov))
    def as(a: String): FreeOperation[GraphTraversal[_, _]] = Free.liftF(AsOperation(a))
    def groupCount(gc: String): FreeOperation[GraphTraversal[_, _]] = Free.liftF(GroupCountOperation(gc))
    def by(b: String): FreeOperation[GraphTraversal[_, _]] = Free.liftF(ByOperation(b))
    def back(b: String): FreeOperation[GraphTraversal[_, _]] = Free.liftF(BackOperation(b))
    def propGreater(p: String, g: Double): FreeOperation[GraphTraversal[_, _]] = Free.liftF(PropGreaterOperation(p, g))
    def propLess(p: String, l: Double): FreeOperation[GraphTraversal[_, _]] = Free.liftF(PropLessOperation(p, l))
    def propEquals(p:String, e: Double): FreeOperation[GraphTraversal[_, _]] = Free.liftF(PropEqualsOperation(p, e))
    def limit(l: Long): FreeOperation[GraphTraversal[_, _]] = Free.liftF(LimitOperation(l))
    def range(b: Long, e: Long): FreeOperation[GraphTraversal[_, _]] = Free.liftF(RangeOperation(b, e))
    def select(s: List[String]): FreeOperation[GraphTraversal[_, _]] = Free.liftF(SelectOperation(s))
    def count(c: String): FreeOperation[GraphTraversal[_, Long]] = Free.liftF(CountOperation(c))
    def dedup(c: String): FreeOperation[GraphTraversal[_, _]] = Free.liftF(DedupOperation(c))
  }

  def operationInterpreter(traversal: GraphTraversal[_, _]): (Operation ~> Id) =
    new (Operation ~> Id) {
      def apply[A](input: Operation[A]): Id[A] =
        input match {
          case IdentityOperation => traversal.asInstanceOf[A]
          case LabelOperation(label) => traversal.hasLabel(label).asInstanceOf[A]
          case HasOperation(has, within) => {
            if (within.isEmpty) {
              traversal.has(has).asInstanceOf[A]
            } else {
              traversal.has(has, P.within(within: _*)).asInstanceOf[A]
            }
          }

          case HasNotOperation(hasNot) => traversal.hasNot(hasNot).asInstanceOf[A]
          case ValuesOperation(values) => traversal.values(values: _*).asInstanceOf[A]
          case CapOperation(cap) => traversal.cap(cap.head, cap.tail: _*).asInstanceOf[A]
          case InOperation(in) => traversal.in(in).asInstanceOf[A]
          case OutOperation(out) => traversal.out(out).asInstanceOf[A]
          case InEdgeOperation(inEdge) => traversal.inE(inEdge).asInstanceOf[A]
          case OutEdgeOperation(outEdge) => traversal.outE(outEdge).asInstanceOf[A]
          case InVertexOperation(inVertex) => traversal.inV().asInstanceOf[A]
          case OutVertexOperation(outVertex) => traversal.outV().asInstanceOf[A]
          case AsOperation(as) => traversal.as(as).asInstanceOf[A]
          case GroupCountOperation(groupCount) => {
            if (groupCount == "") {
              traversal.groupCount().asInstanceOf[A]
            } else {
              traversal.groupCount(groupCount).asInstanceOf[A]
            }
          }

          case ByOperation(by) => traversal.by(by).asInstanceOf[A]
          case BackOperation(back) => traversal.select(back).asInstanceOf[A]

          case PropGreaterOperation(prop, gt) => {
            if (prop.isEmpty) {
              traversal.filter(new JPredicate[Traverser[_]] {
                override def test(h: Traverser[_]): Boolean = h.get.asInstanceOf[GraphTraversal[_, _]].is(P.gt(gt)).toList.head.asInstanceOf[Boolean]
              }).asInstanceOf[A]
              // traversal.filter(new JPredicate[Traverser[GraphTraversal[_, _]]] {
              //   override def test(h: Traverser[GraphTraversal[_, _]]): Boolean = h.get.is(P.gt(gt)).toList.head.asInstanceOf[Boolean]
              // }).asInstanceOf[A]
            } else {
              traversal.filter(_.value(prop).is(P.gt(gt))).asInstanceOf[A]
            }
          }

          case PropLessOperation(prop, lt) => {
            if (prop.isEmpty) {
              traversal.filter(_.is(P.lt(lt))).asInstanceOf[A]
            } else {
              traversal.filter(_.value(prop).is(P.lt(lt))).asInstanceOf[A]
            }
          }

          case PropEqualsOperation(prop, eq) => {
            if (prop.isEmpty) {
              traversal.filter(_.is(P.eq(eq))).asInstanceOf[A]
            } else {
              traversal.filter(_.value(prop).is(P.eq(eq))).asInstanceOf[A]
            }
          }

          case LimitOperation(limit) => traversal.limit(limit).asInstanceOf[A]
          case RangeOperation(begin, end) => traversal.range(begin, end).asInstanceOf[A]
          case CountOperation(count) => traversal.count().asInstanceOf[A]
          case DedupOperation(dedup) => traversal.dedup().asInstanceOf[A]
          case SelectOperation(select) => {
            if (select.isEmpty) {
              traversal
            } else if (select.size == 1) {
              traversal.select[Any](select.head).asInstanceOf[A]
            } else {
              traversal.select[Any](select.head, select.tail.head, select.tail.tail: _*)
            }
          }
        }
    }

  def groupAs[A, B, C](items: Iterable[A]) (key: A => B) (value: A => C): Map[B, List[C]] = {
    items.foldLeft(Map[B, List[C]]()) { (group, item) =>
      val k = key(item)
      group + (k -> (value(item) :: group.getOrElse(k, List[C]())))
    }
  }

  type EdgeMap = Map[String, List[String]]

  sealed trait GraphView
  case class VertexDirect(`type`: String, properties: Map[String, Any]) extends GraphView
  case class VertexFull(`type`: String, properties: Map[String, Any], in: EdgeMap, out: EdgeMap) extends GraphView
  case class EdgeDirect(`type`: String, properties: Map[String, Any]) extends GraphView
  case class EdgeFull(`type`: String, properties: Map[String, Any], in: String, out: String) extends GraphView

  object GraphView {
    def valueMap(vertex: Vertex): Map[String, Any] = {
      val properties = vertex.properties[Any](vertex.keys.toList: _*)
      properties.map((vp: VertexProperty[Any]) => (vp.key, vp.value)).toMap
    }

    def valueMap(edge: Edge): Map[String, Any] = {
      val properties = edge.properties[Any](edge.keys.toList: _*)
      properties.map((vp: Property[Any]) => (vp.key, vp.value)).toMap
    }

    def inMap(vertex: Vertex): EdgeMap = {
      val edges = vertex.graph.traversal.V(vertex.id).inE().toList
      groupAs[Edge, String, String](edges) (_.label) (_.outVertex.value[String]("gid"))
    }

    def outMap(vertex: Vertex): EdgeMap = {
      val edges = vertex.graph.traversal.V(vertex.id).outE().toList
      groupAs[Edge, String, String](edges) (_.label) (_.inVertex.value[String]("gid"))
    }

    def translateVertex(vertex: Vertex): GraphView = VertexDirect(vertex.label, valueMap(vertex))
    def translateEdge(edge: Edge): GraphView = EdgeDirect(edge.label, valueMap(edge))
  }

  case class Query(query: List[Operation[_]]) extends Operation[GraphTraversal[_, _]] {
    def compose: FreeOperation[List[GraphTraversal[_, _]]] = {
      query.map(operation => Free.liftF(operation).asInstanceOf[FreeOperation[GraphTraversal[_, _]]]).sequenceU
    }

    def interpret(traversal: GraphTraversal[_, _]): GraphTraversal[_, _] = {
      compose.foldMap(operationInterpreter(traversal)).toList.head
    }

    def run(graph: Graph): List[Any] = {
      val traversal = graph.traversal.V()
      interpret(traversal).toList.toList
    }
  }

  object Query {
    class OperationSerializer extends CustomSerializer[Operation[_]](format => ({
      case JObject(List(JField("label", JString(label)))) => LabelOperation(label)
      case JObject(List(JField("has", JString(has)))) => HasOperation(has, List())
      case JObject(List(JField("has", JString(has)), JField("within", within))) => HasOperation(has, within.extract[List[_]])
      case JObject(List(JField("within", within), JField("has", JString(has)))) => HasOperation(has, within.extract[List[_]])
      case JObject(List(JField("hasNot", JString(hasNot)))) => HasNotOperation(hasNot)
      case JObject(List(JField("values", values))) => ValuesOperation(values.extract[List[String]])
      case JObject(List(JField("cap", cap))) => CapOperation(cap.extract[List[String]])
      case JObject(List(JField("in", JString(in)))) => InOperation(in)
      case JObject(List(JField("out", JString(out)))) => OutOperation(out)
      case JObject(List(JField("inVertex", JString(inVertex)))) => InVertexOperation(inVertex)
      case JObject(List(JField("outVertex", JString(outVertex)))) => OutVertexOperation(outVertex)
      case JObject(List(JField("inEdge", JString(inEdge)))) => InEdgeOperation(inEdge)
      case JObject(List(JField("outEdge", JString(outEdge)))) => OutEdgeOperation(outEdge)
      case JObject(List(JField("as", JString(as)))) => AsOperation(as)
      case JObject(List(JField("groupCount", JString(groupCount)))) => GroupCountOperation(groupCount)
      case JObject(List(JField("by", JString(by)))) => ByOperation(by)
      case JObject(List(JField("back", JString(back)))) => BackOperation(back)

      // case JObject(List(JField("prop", JString(prop)), JField("gt", JLong(gt)))) => PropGreaterOperation(prop, gt.toLong)
      case JObject(List(JField("prop", JString(prop)), JField("gt", JDouble(gt)))) => PropGreaterOperation(prop, gt.toDouble)
      // case JObject(List(JField("gt", JLong(gt)), JField("prop", JString(prop)))) => PropGreaterOperation(prop, gt.toLong)
      case JObject(List(JField("gt", JDouble(gt)), JField("prop", JString(prop)))) => PropGreaterOperation(prop, gt.toDouble)

      // case JObject(List(JField("prop", JString(prop)), JField("lt", JLong(lt)))) => PropLessOperation(prop, lt.toLong)
      case JObject(List(JField("prop", JString(prop)), JField("lt", JDouble(lt)))) => PropLessOperation(prop, lt.toDouble)
      // case JObject(List(JField("lt", JLong(lt)), JField("prop", JString(prop)))) => PropLessOperation(prop, lt.toLong)
      case JObject(List(JField("lt", JDouble(lt)), JField("prop", JString(prop)))) => PropLessOperation(prop, lt.toDouble)

      // case JObject(List(JField("prop", JString(prop)), JField("eq", JLong(eq)))) => PropEqualsOperation(prop, eq.toLong)
      case JObject(List(JField("prop", JString(prop)), JField("eq", JDouble(eq)))) => PropEqualsOperation(prop, eq.toDouble)
      // case JObject(List(JField("prop", JString(prop)), JField("eq", JString(eq)))) => PropEqualsOperation(prop, eq.toString)
      // case JObject(List(JField("eq", JLong(eq)), JField("prop", JString(prop)))) => PropEqualsOperation(prop, eq.toLong)
      case JObject(List(JField("eq", JDouble(eq)), JField("prop", JString(prop)))) => PropEqualsOperation(prop, eq.toDouble)
      // case JObject(List(JField("eq", JString(eq)), JField("prop", JString(prop)))) => PropEqualsOperation(prop, eq.toString)

      case JObject(List(JField("limit", JLong(limit)))) => LimitOperation(limit)
      case JObject(List(JField("limit", JInt(limit)))) => LimitOperation(limit.toLong)

      case JObject(List(JField("begin", JLong(begin)), JField("end", JLong(end)))) => RangeOperation(begin, end)
      case JObject(List(JField("end", JLong(end)), JField("begin", JLong(begin)))) => RangeOperation(begin, end)
      case JObject(List(JField("begin", JInt(begin)), JField("end", JInt(end)))) => RangeOperation(begin.toLong, end.toLong)
      case JObject(List(JField("end", JInt(end)), JField("begin", JInt(begin)))) => RangeOperation(begin.toLong, end.toLong)

      case JObject(List(JField("select", select))) => SelectOperation(select.extract[List[String]])
      case JObject(List(JField("count", JString(count)))) => CountOperation(count)
      case JObject(List(JField("dedup", JString(dedup)))) => DedupOperation(dedup)
    }, {
      case LabelOperation(label) => JObject(JField("label", JString(label)))
      // case HasOperation(has, within) => JObject(JField("has", JString(has)), JField("within", JArray(within)))
      case InOperation(in) => JObject(JField("in", JString(in)))
    }))

    implicit val formats = Serialization.formats(NoTypeHints) + new OperationSerializer()

    def resultJson(result: List[Any]): JValue = {
      val translation = result.map { item => 
        item match {
          case item: Vertex => GraphView.translateVertex(item)
          case item: Edge => GraphView.translateEdge(item)
          case item: java.util.HashMap[String, Any] => item.toMap
          case item: java.util.LinkedHashMap[String, Any] => {
            val map = item.toMap
            map.mapValues { subitem =>
              subitem match {
                case item: Vertex => GraphView.translateVertex(item)
                case item: Edge => GraphView.translateEdge(item)
                case item: java.util.HashMap[String, Any] => item.toMap
                case _ => item
              }
            }
          }

          case _ => item
        }
      }

      val output = Map("result" -> translation)
      Extraction.decompose(output)
    }

    def fromJson(json: JValue): Query = {
      json.extract[Query]
    }

    def fromString(raw: String): Query = {
      val json = parse(raw)
      fromJson(json)
    }

    def toJson(query: Query): JValue = {
      Extraction.decompose(query)
    }
  }
}

// trait Monad[M[_]] {
//   def pure[A](a: A): M[A]
//   def flatMap[A, B](ma: M[A]) (f: A => M[B]): M[B]
// }

// sealed trait ~>[F[_], G[_]] {
//   def apply[A](f: F[A]): G[A]
// }

// sealed trait Free[F[_], A] {
//   def flatMap[B](f: A => Free[F, B]): Free[F, B] =
//     this match {
//       case Return(a) => f(a)
//       case Bind(i, k) => Bind(i, k andThen (_ flatMap f))
//     }

//   def map[B](f: A => B): Free[F, B] =
//     flatMap(a => Bind(f(a)))

//   def foldMap[G[_]: Monad](f: F ~> G): G[A] =
//     this match {
//       case Return(a) => Monad[G].pure(a)
//       case Bind(fx, g) =>
//         Monad[G].flatMap(f(fx)) { a =>
//           g(a).foldMap(f)
//         }
//     }

//   implicit def lift[F[_], A](fa: F[A]): Free[F, A] =
//     Bind(fa, (a: A) => Return(a))
// }

// case class Return[F[_], A](a: A) extends Free[F, A]
// case class Bind[F[_], I, A](i: F[I], k: I => Free[F, A]) extends Free[F, A]

