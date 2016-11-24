This test currently fails to compile:

    > sbt test

    [error] leprechaun/src/test/scala/leprechaun/test/LeprechaunTest.scala:33: could not find implicit value for parameter p: shapeless.ops.hlist.Prepend[In,shapeless.::[G,shapeless.HNil]]
    [error]     val result: GremlinScala[Vertex, HList] = query.operate(graph)

    [error] src/main/scala/leprechaun/Leprechaun.scala:74: could not find implicit value for parameter p: shapeless.ops.hlist.Prepend[In,shapeless.::[A,shapeless.HNil]]
    [error]   implicit def as[T, L <: HList, A, In <: HList] = at[AsOperation[A, In], GremlinScala[A, In]] {(t, acc) => t.operate(acc)}