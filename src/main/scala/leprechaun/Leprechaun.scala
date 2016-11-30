package leprechaun

import shapeless.{::,HNil,HList}
// import shapeless.ops.hlist.RightFolder
// import shapeless.ops.hlist.Prepend

import cats.free.Free
import cats.{~>,Id}

import gremlin.scala._
import org.apache.tinkerpop.gremlin.process.traversal.P
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

object Operation {
  type OperationFree[F] = Free[Operation, F]

  sealed trait Operation[O]
  final case class VertexOperation(vertex: String) extends Operation[GremlinScala[Vertex, HNil]]
  // final case class HasOperation[M](has: String, within: List[M], graph: GremlinScala[Vertex, HList]) extends Operation[GremlinScala[Vertex, HList]]
  final case class InOperation(in: String) extends Operation[GremlinScala[Vertex, HNil]]

  object Operation {
    def vertex(v: String): OperationFree[GremlinScala[Vertex, HNil]] = Free.liftF(VertexOperation(v))
    // def has[M](h: String, w: List[M]): Free[Operation, GremlinScala[Vertex, HList]] = Free.liftF(HasOperation(h, w))
    def in(i: String): OperationFree[GremlinScala[Vertex, HNil]] = Free.liftF(InOperation(i))
  }

  def operationInterpreter(graph: GremlinScala[Vertex, HNil]): (Operation ~> Id) =
    new (Operation ~> Id) {
      // import Operation._
      def apply[A](input: Operation[A]): Id[A] =
        input match {
          case VertexOperation(vertex) => graph.hasLabel(vertex).asInstanceOf[A]
          // case HasOperation(has, within, graph) => {
          //   val gid = Key[M](has)
          //   IO(graph.has(gid, P.within(within:_*)))
          // }
          case InOperation(in) => graph.in(in).asInstanceOf[A]
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

