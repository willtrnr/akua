package akua

import akka.stream.{FanInShape, Inlet, Outlet}

class JoinShape[L, R](_init: FanInShape.Init[JoinShape.Full[L, R]] = FanInShape.Name[JoinShape.Full[L, R]]("Join")) extends FanInShape[JoinShape.Full[L, R]](_init) {

  protected override def construct(init: FanInShape.Init[JoinShape.Full[L, R]]): JoinShape[L, R] = new JoinShape(init)

  val left: Inlet[L] = newInlet[L]("left")
  val right: Inlet[R] = newInlet[R]("right")

}

object JoinShape {

  type Full[L, R] = (Option[L], Option[R])
  type Inner[L, R] = (L, R)
  type Left[L, R] = (L, Option[R])
  type Right[L, R] = (Option[L], R)
  type Outer[L, R] = Either[L, R]

  def apply[L, R](left: Inlet[L], right: Inlet[R], out: Outlet[Full[L, R]]): JoinShape[L, R] =
    new JoinShape(FanInShape.Ports(out, left :: right :: Nil))

}
