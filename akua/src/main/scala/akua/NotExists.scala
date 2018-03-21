package akua

import scala.collection.mutable

import akka.NotUsed
import akka.stream.scaladsl.{Flow, GraphDSL, SubFlow, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Graph, Inlet, Outlet, SourceShape}

private[akua] final class NotExists[A, B](extractKey: A => B) extends GraphStage[JoinShape[A, B, A]] {

  val left: Inlet[A] = Inlet("NotExists.left")
  val right: Inlet[B] = Inlet("NotExists.right")
  val out: Outlet[A] = Outlet("NotExists.out")

  override val shape: JoinShape[A, B, A] = JoinShape(left, right, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with OutHandler {

    private[this] val keys: mutable.Set[B] = mutable.Set.empty

    override def preStart(): Unit = {
      pull(right)
    }

    setHandler(left, new InHandler {

      override def onPush(): Unit = {
        val e = grab(left)
        val k = extractKey(e)
        if (!keys(k)) {
          push(out, e)
        } else {
          pull(left)
        }
      }

      override def onUpstreamFinish(): Unit = {
        completeStage()
      }

    })

    setHandler(right, new InHandler {

      override def onPush(): Unit = {
        keys += grab(right)
        pull(right)
      }

      override def onUpstreamFinish(): Unit = {
        if (isAvailable(out)) pull(left)
      }

    })

    override def onPull(): Unit = {
      if (isClosed(right)) pull(left)
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
      keys ~> e.right
      FlowShape(e.left, e.out)
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
      k ~> e.right
      FlowShape(e.left, e.out)
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
