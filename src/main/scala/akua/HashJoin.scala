package akua

import scala.collection.mutable

import akka.NotUsed
import akka.stream.scaladsl.{Flow, GraphDSL, SubFlow, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Graph, Inlet, Outlet, SourceShape}

private[akua] final class HashJoin[L, R, A](extractKeyL: L => A, extractKeyR: R => A) extends GraphStage[JoinShape[L, R, JoinShape.Full[L, R]]] {

  val left: Inlet[L] = Inlet("HashJoin.left")
  val right: Inlet[R] = Inlet("HashJoin.right")
  val out: Outlet[JoinShape.Full[L, R]] = Outlet("HashJoin.out")

  override val shape: JoinShape[L, R, JoinShape.Full[L, R]] = JoinShape(left, right, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with OutHandler {

    private[this] val table: mutable.Map[A, Vector[R]] = mutable.Map.empty
    private[this] val unmatched: mutable.Set[A] = mutable.Set.empty

    override def preStart(): Unit = {
      pull(right)
    }

    private[this] def drain(): Unit = {
      emitMultiple(
        out,
        unmatched.toStream
          .flatMap(table.apply)
          .map(v => (None, Some(v))),
        () => completeStage()
      )
    }

    setHandler(left, new InHandler {

      override def onPush(): Unit = {
        val e = grab(left)
        val k = extractKeyL(e)
        table.get(k) match {
          case Some(l) =>
            unmatched -= k
            emitMultiple(out, l.map(v => (Some(e), Some(v))))
          case None =>
            push(out, (Some(e), None))
        }
      }

      override def onUpstreamFinish(): Unit = {
        if (!isClosed(out)) {
          if (isClosed(right)) drain()
        } else {
          completeStage()
        }
      }

    })

    setHandler(right, new InHandler {

      override def onPush(): Unit = {
        val e = grab(right)
        val k = extractKeyR(e)
        table += (k -> (table.getOrElse(k, Vector.empty) :+ e))
        unmatched += k
        pull(right)
      }

      override def onUpstreamFinish(): Unit = {
        if (!isClosed(out)) {
          if (!isClosed(left)) {
            if (isAvailable(out)) pull(left)
          } else {
            drain()
          }
        } else {
          completeStage()
        }
      }

    })

    override def onPull(): Unit = {
      if (isClosed(right) && !isClosed(left)) tryPull(left)
    }

    setHandler(out, this)

  }

}

object HashJoin {

  def apply[L, R, A](lf: L => A, rf: R => A): HashJoin[L, R, A] =
    new HashJoin(lf, rf)

  def full[L, R, A](left: Source[L, _], right: Source[R, _])(lf: L => A, rf: R => A): Source[JoinShape.Full[L, R], NotUsed] =
    Source.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val join = b.add(apply(lf, rf))
      left  ~> join.left
      right ~> join.right
      SourceShape(join.out)
    })

  def inner[L, R, A](left: Source[L, _], right: Source[R, _])(lf: L => A, rf: R => A): Source[JoinShape.Inner[L, R], NotUsed] =
    full(left, right)(lf, rf) collect { case (Some(l), Some(r)) => (l, r) }

  def left[L, R, A](left: Source[L, _], right: Source[R, _])(lf: L => A, rf: R => A): Source[JoinShape.Left[L, R], NotUsed] =
    full(left, right)(lf, rf) collect { case (Some(l), r) => (l, r) }

  def right[L, R, A](left: Source[L, _], right: Source[R, _])(lf: L => A, rf: R => A): Source[JoinShape.Right[L, R], NotUsed] =
    full(left, right)(lf, rf) collect { case (l, Some(r)) => (l, r) }

  def outer[L, R, A](left: Source[L, _], right: Source[R, _])(lf: L => A, rf: R => A): Source[JoinShape.Outer[L, R], NotUsed] =
    full(left, right)(lf, rf) collect {
      case (Some(l), None) => Left(l)
      case (None, Some(r)) => Right(r)
    }

}

private[akua] trait HashJoinOps[Out, Mat] {

  type Repr[O] <: akka.stream.scaladsl.FlowOps[O, Mat] {
    type Repr[OO] <: HashJoinOps.this.Repr[OO]
  }

  protected def self: Repr[Out]

  private[this] def fullJoinGraph[Out2, Mat2, A](right: Graph[SourceShape[Out2], Mat2])(lf: Out => A, rf: Out2 => A): Graph[FlowShape[Out, JoinShape.Full[Out, Out2]], Mat2] =
    GraphDSL.create(right) { implicit b => r =>
      import GraphDSL.Implicits._
      val join = b.add(HashJoin(lf, rf))
      r ~> join.right
      FlowShape(join.left, join.out)
    }

  def fullHashJoin[Out2, A](right: Graph[SourceShape[Out2], _])(lf: Out => A, rf: Out2 => A): Repr[JoinShape.Full[Out, Out2]] =
    self.via(fullJoinGraph(right)(lf, rf))

  def innerHashJoin[Out2, A](right: Graph[SourceShape[Out2], _])(lf: Out => A, rf: Out2 => A): Repr[JoinShape.Inner[Out, Out2]] =
    fullHashJoin(right)(lf, rf) collect { case (Some(l), Some(r)) => (l, r) }

  def leftHashJoin[Out2, A](right: Graph[SourceShape[Out2], _])(lf: Out => A, rf: Out2 => A): Repr[JoinShape.Left[Out, Out2]] =
    fullHashJoin(right)(lf, rf) collect { case (Some(l), r) => (l, r) }

  def rightHashJoin[Out2, A](right: Graph[SourceShape[Out2], _])(lf: Out => A, rf: Out2 => A): Repr[JoinShape.Right[Out, Out2]] =
    fullHashJoin(right)(lf, rf) collect { case (l, Some(r)) => (l, r) }

  def outerHashJoin[Out2, A](right: Graph[SourceShape[Out2], _])(lf: Out => A, rf: Out2 => A): Repr[JoinShape.Outer[Out, Out2]] =
    fullHashJoin(right)(lf, rf) collect {
      case (Some(l), None) => Left(l)
      case (None, Some(r)) => Right(r)
    }

}

final class SourceHashJoinOps[Out, Mat](override protected val self: Source[Out, Mat]) extends HashJoinOps[Out, Mat] {
  override type Repr[O] = Source[O, Mat]
}

final class FlowHashJoinOps[In, Out, Mat](override protected val self: Flow[In, Out, Mat]) extends HashJoinOps[Out, Mat] {
  override type Repr[O] = Flow[In, O, Mat]
}

final class SubFlowHashJoinOps[Out, Mat, F[+_], C](override protected val self: SubFlow[Out, Mat, F, C]) extends HashJoinOps[Out, Mat] {
  override type Repr[O] = SubFlow[O, Mat, F, C]
}

trait ToHashJoinOps {
  implicit def toSourceHashJoinOps[Out, Mat](source: Source[Out, Mat]): SourceHashJoinOps[Out, Mat] = new SourceHashJoinOps(source)
  implicit def toFlowHashJoinOps[In, Out, Mat](flow: Flow[In, Out, Mat]): FlowHashJoinOps[In, Out, Mat] = new FlowHashJoinOps(flow)
  implicit def toSubFlowHashJoinOps[Out, Mat, F[+_], C](sub: SubFlow[Out, Mat, F, C]): SubFlowHashJoinOps[Out, Mat, F, C] = new SubFlowHashJoinOps(sub)
}
