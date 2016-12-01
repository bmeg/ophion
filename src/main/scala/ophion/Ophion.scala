package ophion

import cats._
import cats.implicits._
import cats.free.Free

import org.apache.tinkerpop.gremlin.structure.{Vertex, Edge}
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal

import org.json4s._
import org.json4s.jackson._
import org.json4s.jackson.JsonMethods._

object Ophion {
  type FreeOperation[F] = Free[Operation, F]

  sealed trait Operation[O]
  case object IdentityOperation extends Operation[GraphTraversal[_, _]]
  case class LabelOperation(label: String) extends Operation[GraphTraversal[_, Vertex]]
  case class HasOperation(has: String, within: List[_]) extends Operation[GraphTraversal[_, Vertex]]
  case class InOperation(in: String) extends Operation[GraphTraversal[_, Vertex]]
  case class OutOperation(out: String) extends Operation[GraphTraversal[_, Vertex]]
  case class InEdgeOperation(inEdge: String) extends Operation[GraphTraversal[_, Edge]]
  case class OutEdgeOperation(outEdge: String) extends Operation[GraphTraversal[_, Edge]]
  case class InVertexOperation(inVertex: String) extends Operation[GraphTraversal[_, Vertex]]
  case class OutVertexOperation(outVertex: String) extends Operation[GraphTraversal[_, Vertex]]
  case class AsOperation(as: String) extends Operation[GraphTraversal[_, _]]
  case class SelectOperation(select: List[String]) extends Operation[GraphTraversal[_, _]]

  object Operation {
    def identity: FreeOperation[GraphTraversal[_, _]] = Free.liftF(IdentityOperation)
    def label(v: String): FreeOperation[GraphTraversal[_, Vertex]] = Free.liftF(LabelOperation(v))
    def has(h: String, w: List[_]): FreeOperation[GraphTraversal[_, Vertex]] = Free.liftF(HasOperation(h, w))
    def in(i: String): FreeOperation[GraphTraversal[_, Vertex]] = Free.liftF(InOperation(i))
    def out(o: String): FreeOperation[GraphTraversal[_, Vertex]] = Free.liftF(OutOperation(o))
    def inEdge(ie: String): FreeOperation[GraphTraversal[_, Edge]] = Free.liftF(InEdgeOperation(ie))
    def outEdge(oe: String): FreeOperation[GraphTraversal[_, Edge]] = Free.liftF(OutEdgeOperation(oe))
    def inVertex(iv: String): FreeOperation[GraphTraversal[_, Vertex]] = Free.liftF(InVertexOperation(iv))
    def outVertex(ov: String): FreeOperation[GraphTraversal[_, Vertex]] = Free.liftF(OutVertexOperation(ov))
    def as(a: String): FreeOperation[GraphTraversal[_, _]] = Free.liftF(AsOperation(a))
    def select(s: List[String]): FreeOperation[GraphTraversal[_, _]] = Free.liftF(SelectOperation(s))
  }

  def operationInterpreter(traversal: GraphTraversal[_, _]): (Operation ~> Id) =
    new (Operation ~> Id) {
      def apply[A](input: Operation[A]): Id[A] =
        input match {
          case IdentityOperation => traversal.asInstanceOf[A]
          case LabelOperation(label) => traversal.hasLabel(label).asInstanceOf[A]
          case HasOperation(has, within) => traversal.has(has, P.within(within: _*)).asInstanceOf[A]
          case InOperation(in) => traversal.in(in).asInstanceOf[A]
          case OutOperation(out) => traversal.out(out).asInstanceOf[A]
          case InEdgeOperation(inEdge) => traversal.inE(inEdge).asInstanceOf[A]
          case OutEdgeOperation(outEdge) => traversal.outE(outEdge).asInstanceOf[A]
          case InVertexOperation(inVertex) => traversal.inV().asInstanceOf[A]
          case OutVertexOperation(outVertex) => traversal.outV().asInstanceOf[A]
          case AsOperation(as) => traversal.as(as).asInstanceOf[A]
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

  case class Query(query: List[Operation[_]]) extends Operation[GraphTraversal[_, _]] {
    def compose: FreeOperation[List[GraphTraversal[_, _]]] = {
      query.map(operation => Free.liftF(operation).asInstanceOf[FreeOperation[GraphTraversal[_, _]]]).sequenceU
    }
  }

  object Query {
    class OperationSerializer extends CustomSerializer[Operation[_]](format => ({
      case JObject(List(JField("label", JString(label)))) => LabelOperation(label)
      case JObject(List(JField("has", JString(has)), JField("within", within))) => HasOperation(has, within.extract[List[_]])
      case JObject(List(JField("in", JString(in)))) => InOperation(in)
      case JObject(List(JField("out", JString(out)))) => OutOperation(out)
      case JObject(List(JField("inVertex", JString(inVertex)))) => InVertexOperation(inVertex)
      case JObject(List(JField("outVertex", JString(outVertex)))) => OutVertexOperation(outVertex)
      case JObject(List(JField("inEdge", JString(inEdge)))) => InEdgeOperation(inEdge)
      case JObject(List(JField("outEdge", JString(outEdge)))) => OutEdgeOperation(outEdge)
      case JObject(List(JField("as", JString(as)))) => AsOperation(as)
      case JObject(List(JField("select", select))) => SelectOperation(select.extract[List[String]])
    }, {
      case LabelOperation(label) => JObject(JField("label", JString(label)))
      // case HasOperation(has, within) => JObject(JField("has", JString(has)), JField("within", JArray(within)))
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

