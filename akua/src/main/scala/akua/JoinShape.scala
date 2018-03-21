package akua

import akka.stream.{FanInShape, Inlet, Outlet}

class JoinShape[L, R, O](_init: FanInShape.Init[O] = FanInShape.Name[O]("Join")) extends FanInShape[O](_init) {

  protected override def construct(init: FanInShape.Init[O]): JoinShape[L, R, O] = new JoinShape(init)

  val left: Inlet[L] = newInlet[L]("left")
  val right: Inlet[R] = newInlet[R]("right")

}

object JoinShape {

  type Full[L, R] = (Option[L], Option[R])
  type Inner[L, R] = (L, R)
  type Left[L, R] = (L, Option[R])
  type Right[L, R] = (Option[L], R)
  type Outer[L, R] = Either[L, R]

  def apply[L, R, O](left: Inlet[L], right: Inlet[R], out: Outlet[O]): JoinShape[L, R, O] =
    new JoinShape(FanInShape.Ports(out, left :: right :: Nil))

}
