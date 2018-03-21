package akua

import akka.stream.scaladsl.{Flow, SubFlow, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

private[akua] final class FoldBy[A, B, K](getKey: A => K, zero: A => B, f: (B, A) => B) extends GraphStage[FlowShape[A, (K, B)]] {

  val in: Inlet[A] = Inlet("FoldBy.in")
  val out: Outlet[(K, B)] = Outlet("FoldBy.out")

  override val shape: FlowShape[A, (K, B)] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    def folding(key: K, agg: B): InHandler with OutHandler = new InHandler with OutHandler {

      override def onPush(): Unit = {
        val e = grab(in)
        val k = getKey(e)
        if (k != key) {
          setHandlers(in, out, folding(k, zero(e)))
          push(out, (key, agg))
        } else {
          setHandlers(in, out, folding(key, f(agg, e)))
          pull(in)
        }
      }

      override def onUpstreamFinish(): Unit = {
        emit(out, (key, agg), () => completeStage())
      }

      override def onPull(): Unit = {
        pull(in)
      }

    }

    setHandlers(in, out, new InHandler with OutHandler {

      override def onPush(): Unit = {
        val e = grab(in)
        val k = getKey(e)
        setHandlers(in, out, folding(k, zero(e)))
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        completeStage()
      }

      override def onPull(): Unit = {
        pull(in)
      }

    })

  }

}

object FoldBy {

  def apply[A, B, K](getKey: A => K, zero: A => B)(f: (B, A) => B): FoldBy[A, B, K] =
    new FoldBy(getKey, zero, f)

  def apply[A, K](getKey: A => K)(f: (A, A) => A): FoldBy[A, A, K] =
    apply(getKey, identity[A])(f)

  def grouping[A, K](getKey: A => K): FoldBy[A, Vector[A], K] =
    apply[A, Vector[A], K](getKey, Vector(_))(_ :+ _)

}

private[akua] trait FoldByOps[Out, Mat] {

  type Repr[O] <: akka.stream.scaladsl.FlowOps[O, Mat] {
    type Repr[OO] <: FoldByOps.this.Repr[OO]
  }

  protected def self: Repr[Out]

  def foldBy[A, B](getKey: Out => A, zero: Out => B)(f: (B, Out) => B): Repr[(A, B)] =
    self.via(FoldBy(getKey, zero)(f))

  def foldBy[A](getKey: Out => A)(f: (Out, Out) => Out): Repr[(A, Out)] =
    foldBy(getKey, identity)(f)

}

final class SourceFoldByOps[Out, Mat](override protected val self: Source[Out, Mat]) extends FoldByOps[Out, Mat] {
  override type Repr[O] = Source[O, Mat]
}

final class FlowFoldByOps[In, Out, Mat](override protected val self: Flow[In, Out, Mat]) extends FoldByOps[Out, Mat] {
  override type Repr[O] = Flow[In, O, Mat]
}

final class SubFlowFoldByOps[Out, Mat, F[+_], C](override val self: SubFlow[Out, Mat, F, C]) extends FoldByOps[Out, Mat] {
  override type Repr[O] = SubFlow[O, Mat, F, C]
}

trait ToFoldByOps {
  implicit def toSourceFoldByOps[Out, Mat](source: Source[Out, Mat]): SourceFoldByOps[Out, Mat] = new SourceFoldByOps(source)
  implicit def toFlowFoldByOps[In, Out, Mat](flow: Flow[In, Out, Mat]): FlowFoldByOps[In, Out, Mat] = new FlowFoldByOps(flow)
  implicit def toSubFlowFoldByOps[Out, Mat, F[+_], C](sub: SubFlow[Out, Mat, F, C]): SubFlowFoldByOps[Out, Mat, F, C] = new SubFlowFoldByOps(sub)
}
