package leprechaun

// import shapeless.{::,HNil,HList}
// import shapeless.ops.hlist.RightFolder
// import shapeless.ops.hlist.Prepend

import cats.free.Free
import cats.{~>,Id}

// import gremlin.scala._
import org.apache.tinkerpop.gremlin.structure.{Vertex, Edge}
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal

import org.json4s._
import org.json4s.jackson._
import org.json4s.jackson.JsonMethods._

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

object OperationCoproduct {
  type FreeOperation[F] = Free[Operation, F]

  sealed trait Operation[O]
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
}



// {
//   def operate(input: ScalaGraph): GremlinScala[Vertex, HNil] = {
//     input.V.hasLabel(vertex)
//   }
// }

// {
//   def operate(input: GremlinScala[Vertex, L]): GremlinScala[Vertex, L] = {
//     val gid = Key[M](has)
//     input.has(gid, P.within(within:_*))
//   }
// }

// case class AsOperation(as: String) extends Operation {
//   def operate[A, In <: HList](input: GremlinScala[A, In]) (implicit p: Prepend[In, ::[A, HNil]]): GremlinScala[A, p.Out] = {
//     input.as(as)
//   }
// }

// {
//   def operate(input: GremlinScala[Vertex, L]): GremlinScala[Vertex, L] = {
//     input.in(in)
//   }
// }

// case class OutOperation(out: String) extends Operation {
//   def operate[Labels <: HList](input: GremlinScala[Vertex, Labels]): GremlinScala[Vertex, Labels] = {
//     input.out(out)
//   }
// }

// case class InEdgeOperation(inEdge: String) extends Operation {
//   def operate[Labels <: HList](input: GremlinScala[Vertex, Labels]): GremlinScala[Edge, Labels] = {
//     input.inE(inEdge)
//   }
// }

// case class OutEdgeOperation(outEdge: String) extends Operation {
//   def operate[Labels <: HList](input: GremlinScala[Vertex, Labels]): GremlinScala[Edge, Labels] = {
//     input.outE(outEdge)
//   }
// }

// case class InVertexOperation(inVertex: String) extends Operation {
//   def operate[Labels <: HList](input: GremlinScala[Edge, Labels]): GremlinScala[Vertex, Labels] = {
//     input.inV()
//   }
// }

// case class OutVertexOperation(outVertex: String) extends Operation {
//   def operate[Labels <: HList](input: GremlinScala[Edge, Labels]): GremlinScala[Vertex, Labels] = {
//     input.outV()
//   }
// }

// object Query {
//   class OperationSerializer[I, O] extends CustomSerializer[Operation[I, O]](format => ({
//     case JObject(List(JField("vertex", JString(vertex)))) => VertexOperation(vertex)
//     case JObject(List(JField("has", JString(has)), JField("within", within))) => HasOperation(has, within.extract[List[String]])
//     // case JObject(List(JField("as", JString(as)))) => AsOperation(as)
//     case JObject(List(JField("in", JString(in)))) => InOperation(in)
//     // case JObject(List(JField("out", JString(out)))) => OutOperation(out)
//     // case JObject(List(JField("inVertex", JString(inVertex)))) => InVertexOperation(inVertex)
//     // case JObject(List(JField("outVertex", JString(outVertex)))) => OutVertexOperation(outVertex)
//     // case JObject(List(JField("inEdge", JString(inEdge)))) => InEdgeOperation(inEdge)
//     // case JObject(List(JField("outEdge", JString(outEdge)))) => OutEdgeOperation(outEdge)
//   }, {
//     case VertexOperation(vertex) => JObject(JField("vertex", JString(vertex)))
//     case HasOperation(has, within) => JObject(JField("has", JString(has)), JField("within", JArray(within)))
//     case InOperation(in) => JObject(JField("in", JString(in)))
//   }))

//   implicit val formats = Serialization.formats(NoTypeHints) + new OperationSerializer()

//   def fromJson(json: JValue): Query = {
//     json.extract[Query]
//   }

//   def fromString(raw: String): Query = {
//     val json = parse(raw)
//     fromJson(json)
//   }
// }

// case class Query[I, O](query: List[Operation[I, O]]) extends Operation[I, O] {
//   def operate[ScalaGraph, O]: O
//   // def operate[R, In <: HList, G](graph: ScalaGraph) (implicit p: Prepend[In, ::[G, HNil]]): R = {
//   // def operate[R](graph: ScalaGraph): R = {
//   //   var anvil: Any = graph
//   //   def op[M](operation: Operation) {
//   //     operation match {
//   //       case VertexOperation(vertex) => anvil = operation.asInstanceOf[VertexOperation].operate(anvil.asInstanceOf[ScalaGraph])
//   //       case HasOperation(has, within: List[M]) => anvil = {
//   //         val gid = Key[M](has)
//   //         operation.asInstanceOf[HasOperation[M]].operate(anvil.asInstanceOf[GremlinScala[Vertex, HList]].has(gid, P.within(within:_*)))
//   //       }
//   //       // case AsOperation(as) => anvil = operation.asInstanceOf[AsOperation].operate(anvil.asInstanceOf[GremlinScala[M, HList]])
//   //       case InOperation(in) => anvil = operation.asInstanceOf[InOperation].operate(anvil.asInstanceOf[GremlinScala[Vertex, HList]])
//   //       case OutOperation(out) => anvil = operation.asInstanceOf[OutOperation].operate(anvil.asInstanceOf[GremlinScala[Vertex, HList]])
//   //       case InVertexOperation(inVertex) => anvil = operation.asInstanceOf[InVertexOperation].operate(anvil.asInstanceOf[GremlinScala[Edge, HList]])
//   //       case OutVertexOperation(outVertex) => anvil = operation.asInstanceOf[OutVertexOperation].operate(anvil.asInstanceOf[GremlinScala[Edge, HList]])
//   //       case InEdgeOperation(inEdge) => anvil = operation.asInstanceOf[InEdgeOperation].operate(anvil.asInstanceOf[GremlinScala[Vertex, HList]])
//   //       case OutEdgeOperation(outEdge) => anvil = operation.asInstanceOf[OutEdgeOperation].operate(anvil.asInstanceOf[GremlinScala[Vertex, HList]])
//   //     }
//   //   }

//   //   query.foreach(x => op(x))
//   //   anvil.asInstanceOf[R]
//   // }
// }

// trait ApplyOperationDefault extends Poly2 {
//   implicit def default[T, L <: HList] = at[T, L] ((_, acc) => acc)
// }

// object ApplyOperation extends ApplyOperationDefault {
//   implicit def vertex[T, L <: HList, S <: HList] = at[VertexOperation, ScalaGraph] ((t, acc) => t.operate(acc))
//   implicit def has[M, T, L <: HList, S <: HList] = at[HasOperation[M], GremlinScala[Vertex, S]] ((t, acc) => t.operate(acc))
//   implicit def as[A, T, L <: HList, In <: HList](implicit p: Prepend[In, ::[A, HNil]]) = at[AsOperation, GremlinScala[A, In]] ((t, acc) => t.operate(acc))
//   implicit def in[T, L <: HList, S <: HList] = at[InOperation, GremlinScala[Vertex, S]] ((t, acc) => t.operate(acc))
//   implicit def out[T, L <: HList, S <: HList] = at[OutOperation, GremlinScala[Vertex, S]] ((t, acc) => t.operate(acc))
//   implicit def inEdge[T, L <: HList, S <: HList] = at[InEdgeOperation, GremlinScala[Vertex, S]] ((t, acc) => t.operate(acc))
//   implicit def outEdge[T, L <: HList, S <: HList] = at[OutEdgeOperation, GremlinScala[Vertex, S]] ((t, acc) => t.operate(acc))
//   implicit def inVertex[T, L <: HList, S <: HList] = at[InVertexOperation, GremlinScala[Edge, S]] ((t, acc) => t.operate(acc))
//   implicit def outVertex[T, L <: HList, S <: HList] = at[OutVertexOperation, GremlinScala[Edge, S]] ((t, acc) => t.operate(acc))
// }

// object Operation {
//   def process[Input, Output, A <: HList](operations: A, input: Input) (implicit folder: RightFolder.Aux[A, Input, ApplyOperation.type, Output]): Output = {
//     operations.foldRight(input) (ApplyOperation)
//   }
// }

