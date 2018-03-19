package akua

import akka.NotUsed
import akka.stream.scaladsl.{Flow, GraphDSL, SubFlow, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Graph, Inlet, Outlet, SourceShape}

private[akua] final class MergeJoin[L, R, A](extractKeyL: L => A, extractKeyR: R => A, ord: Ordering[A]) extends GraphStage[JoinShape[L, R]] {

  val left: Inlet[L] = Inlet("MergeJoin.left")
  val right: Inlet[R] = Inlet("MergeJoin.right")
  val out: Outlet[JoinShape.Full[L, R]] = Outlet("MergeJoin.out")

  override val shape: JoinShape[L, R] = JoinShape(left, right, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with OutHandler {

    setHandler(left, new InHandler {
      override def onPush(): Unit = ()
    })

    setHandler(right, new InHandler {
      override def onPush(): Unit = ()
    })

    override def onPull(): Unit = ()

    setHandler(out, this)

  }

}

object MergeJoin {

  def apply[L, R, A](lf: L => A, rf: R => A)(implicit ord: Ordering[A]): MergeJoin[L, R, A] =
    new MergeJoin(lf, rf, ord)

  def full[L, R, A : Ordering](left: Source[L, _], right: Source[R, _])(lf: L => A, rf: R => A): Source[JoinShape.Full[L, R], NotUsed] =
    Source.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val join = b.add(apply(lf, rf))
      left  ~> join.left
      right ~> join.right
      SourceShape(join.out)
    })

  def inner[L, R, A : Ordering](left: Source[L, _], right: Source[R, _])(lf: L => A, rf: R => A): Source[JoinShape.Inner[L, R], NotUsed] =
    full(left, right)(lf, rf) collect { case (Some(l), Some(r)) => (l, r) }

  def left[L, R, A : Ordering](left: Source[L, _], right: Source[R, _])(lf: L => A, rf: R => A): Source[JoinShape.Left[L, R], NotUsed] =
    full(left, right)(lf, rf) collect { case (Some(l), r) => (l, r) }

  def right[L, R, A : Ordering](left: Source[L, _], right: Source[R, _])(lf: L => A, rf: R => A): Source[JoinShape.Right[L, R], NotUsed] =
    full(left, right)(lf, rf) collect { case (l, Some(r)) => (l, r) }

  def outer[L, R, A : Ordering](left: Source[L, _], right: Source[R, _])(lf: L => A, rf: R => A): Source[JoinShape.Outer[L, R], NotUsed] =
    full(left, right)(lf, rf) collect {
      case (Some(l), None) => Left(l)
      case (None, Some(r)) => Right(r)
    }

}

private[akua] trait MergeJoinOps[Out, Mat] {

  type Repr[O] <: akka.stream.scaladsl.FlowOps[O, Mat] {
    type Repr[OO] <: MergeJoinOps.this.Repr[OO]
  }

  protected def self: Repr[Out]

  private[this] def fullJoinGraph[Out2, Mat2, A : Ordering](right: Graph[SourceShape[Out2], Mat2])(lf: Out => A, rf: Out2 => A): Graph[FlowShape[Out, JoinShape.Full[Out, Out2]], Mat2] =
    GraphDSL.create(right) { implicit b => r =>
      import GraphDSL.Implicits._
      val join = b.add(MergeJoin(lf, rf))
      r ~> join.right
      FlowShape(join.left, join.out)
    }

  def fullMergeJoin[Out2, A : Ordering](right: Graph[SourceShape[Out2], _])(lf: Out => A, rf: Out2 => A): Repr[JoinShape.Full[Out, Out2]] =
    self.via(fullJoinGraph(right)(lf, rf))

  def innerMergeJoin[Out2, A : Ordering](right: Graph[SourceShape[Out2], _])(lf: Out => A, rf: Out2 => A): Repr[JoinShape.Inner[Out, Out2]] =
    fullMergeJoin(right)(lf, rf) collect { case (Some(l), Some(r)) => (l, r) }

  def leftMergeJoin[Out2, A : Ordering](right: Graph[SourceShape[Out2], _])(lf: Out => A, rf: Out2 => A): Repr[JoinShape.Left[Out, Out2]] =
    fullMergeJoin(right)(lf, rf) collect { case (Some(l), r) => (l, r) }

  def rightMergeJoin[Out2, A : Ordering](right: Graph[SourceShape[Out2], _])(lf: Out => A, rf: Out2 => A): Repr[JoinShape.Right[Out, Out2]] =
    fullMergeJoin(right)(lf, rf) collect { case (l, Some(r)) => (l, r) }

  def outerMergeJoin[Out2, A : Ordering](right: Graph[SourceShape[Out2], _])(lf: Out => A, rf: Out2 => A): Repr[JoinShape.Outer[Out, Out2]] =
    fullMergeJoin(right)(lf, rf) collect {
      case (Some(l), None) => Left(l)
      case (None, Some(r)) => Right(r)
    }

}

final class SourceMergeJoinOps[Out, Mat](override protected val self: Source[Out, Mat]) extends MergeJoinOps[Out, Mat] {
  override type Repr[O] = Source[O, Mat]
}

final class FlowMergeJoinOps[In, Out, Mat](override protected val self: Flow[In, Out, Mat]) extends MergeJoinOps[Out, Mat] {
  override type Repr[O] = Flow[In, O, Mat]
}

final class SubFlowMergeJoinOps[Out, Mat, F[+_], C](override protected val self: SubFlow[Out, Mat, F, C]) extends MergeJoinOps[Out, Mat] {
  override type Repr[O] = SubFlow[O, Mat, F, C]
}

trait ToMergeJoinOps {
  implicit def toSourceMergeJoinOps[Out, Mat](source: Source[Out, Mat]): SourceMergeJoinOps[Out, Mat] = new SourceMergeJoinOps(source)
  implicit def toFlowMergeJoinOps[In, Out, Mat](flow: Flow[In, Out, Mat]): FlowMergeJoinOps[In, Out, Mat] = new FlowMergeJoinOps(flow)
  implicit def toSubFlowMergeJoinOps[Out, Mat, F[+_], C](sub: SubFlow[Out, Mat, F, C]): SubFlowMergeJoinOps[Out, Mat, F, C] = new SubFlowMergeJoinOps(sub)
}
