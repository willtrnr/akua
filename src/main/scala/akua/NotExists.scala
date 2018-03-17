package akua

import scala.collection.mutable

import akka.NotUsed
import akka.stream.scaladsl.{Flow, GraphDSL, SubFlow, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FanInShape2, FlowShape, Graph, Inlet, Outlet, SourceShape}

private[akua] final class NotExists[A, B](extractKey: A => B) extends GraphStage[FanInShape2[A, B, A]] {

  val in0: Inlet[A] = Inlet("NotExists.in0")
  val in1: Inlet[B] = Inlet("NotExists.in1")
  val out: Outlet[A] = Outlet("NotExists.out")

  override val shape: FanInShape2[A, B, A] = new FanInShape2(in0, in1, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with OutHandler {

    private[this] val keys: mutable.Set[B] = mutable.Set.empty

    override def preStart(): Unit = {
      pull(in1)
    }

    setHandler(in0, new InHandler {

      override def onPush(): Unit = {
        val e = grab(in0)
        val k = extractKey(e)
        if (!keys(k)) {
          push(out, e)
        } else {
          pull(in0)
        }
      }

      override def onUpstreamFinish(): Unit = {
        completeStage()
      }

    })

    setHandler(in1, new InHandler {

      override def onPush(): Unit = {
        keys += grab(in1)
        pull(in1)
      }

      override def onUpstreamFinish(): Unit = {
        if (isAvailable(out)) pull(in0)
      }

    })

    override def onPull(): Unit = {
      if (isClosed(in1)) pull(in0)
    }

    setHandler(out, this)

  }

}

object NotExists {

  def apply[A, B](f: A => B): NotExists[A, B] = new NotExists(f)

  def apply[A, B](keys: Source[B, _])(f: A => B): Flow[A, A, NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val e = b.add(apply(f))
      keys ~> e.in1
      FlowShape(e.in0, e.out)
    })

  def apply[A, B, Mat](values: Source[A, Mat], keys: Source[B, _])(f: A => B): Source[A, Mat] =
    values.via(apply(keys)(f))

}

private[akua] trait NotExistsOps[Out, Mat] {

  type Repr[O] <: akka.stream.scaladsl.FlowOps[O, Mat] { type Repr[OO] = NotExistsOps.this.Repr[OO] }

  protected def self: Repr[Out]

  private[this] def notExistsGraph[Out2, M](keys: Graph[SourceShape[Out2], M])(f: Out => Out2): Graph[FlowShape[Out, Out], M] =
    GraphDSL.create(keys) { implicit b => k =>
      import GraphDSL.Implicits._
      val e = b.add(NotExists(f))
      k ~> e.in1
      FlowShape(e.in0, e.out)
    }

  def notExists[Out2, A](keys: Graph[SourceShape[Out2], _])(f: Out => Out2): Repr[Out] =
    self.via(notExistsGraph(keys)(f))

}

final class SourceNotExistsOps[Out, Mat](override protected val self: Source[Out, Mat]) extends NotExistsOps[Out, Mat] {
  override type Repr[O] = Source[O, Mat]
}

final class FlowNotExistsOps[In, Out, Mat](override protected val self: Flow[In, Out, Mat]) extends NotExistsOps[Out, Mat] {
  override type Repr[O] = Flow[In, O, Mat]
}

final class SubFlowNotExistsOps[Out, Mat, F[+_], C](override protected val self: SubFlow[Out, Mat, F, C]) extends NotExistsOps[Out, Mat] {
  override type Repr[O] = SubFlow[O, Mat, F, C]
}

trait ToNotExistsOps {
  implicit def toSourceNotExistsOps[Out, Mat](source: Source[Out, Mat]): SourceNotExistsOps[Out, Mat] = new SourceNotExistsOps(source)
  implicit def toFlowNotExistsOps[In, Out, Mat](flow: Flow[In, Out, Mat]): FlowNotExistsOps[In, Out, Mat] = new FlowNotExistsOps(flow)
  implicit def toSubFlowNotExistsOps[Out, Mat, F[+_], C](sub: SubFlow[Out, Mat, F, C]): SubFlowNotExistsOps[Out, Mat, F, C] = new SubFlowNotExistsOps(sub)
}
