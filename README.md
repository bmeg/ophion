This currently fails to compile:

    > sbt console

    [error] src/main/scala/leprechaun/Leprechaun.scala:74: could not find implicit value for parameter p: shapeless.ops.hlist.Prepend[In,shapeless.::[A,shapeless.HNil]]
    [error]   implicit def as[T, L <: HList, A, In <: HList] = at[AsOperation[A, In], GremlinScala[A, In]] {(t, acc) => t.operate(acc)}