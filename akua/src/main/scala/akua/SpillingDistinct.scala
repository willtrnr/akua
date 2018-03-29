package akua

import scala.util.control.NonFatal

import java.util.{Set => JSet}

import akka.stream.scaladsl.{Flow, Source, SubFlow}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

import org.mapdb.{DB, DBMaker, Serializer}

private[akua] final class SpillingDistinct[A, K](ser: Serializer[K], extractKey: A => K) extends GraphStage[FlowShape[A, A]] {

  val in: Inlet[A] = Inlet("SpillingDistinct.in")
  val out: Outlet[A] = Outlet("SpillingDistinct.out")

  override val shape: FlowShape[A, A] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {

    private[this] var db: DB = _
    private[this] var seen: JSet[K] = _

    override def preStart(): Unit = {
      db = DBMaker.tempFileDB()
        .fileMmapEnableIfSupported()
        .fileMmapPreclearDisable()
        .cleanerHackEnable()
        .concurrencyDisable() // Akka Stream guarantees that callbacks are synchronized
        .make()

      try {
        seen = db.hashSet("seen")
          .serializer(ser)
          .create()
      } catch {
        case NonFatal(e) =>
          db.close()
          throw e
      }
    }

    override def postStop(): Unit = {
      if (db ne null) db.close()
    }

    override def onPush(): Unit = {
      val e = grab(in)
      val k = extractKey(e)
      if (seen.contains(k)) {
        pull(in)
      } else {
        seen.add(k)
        push(out, e)
      }
    }

    override def onPull(): Unit = {
      pull(in)
    }

    setHandlers(in, out, this)

  }

}

object SpillingDistinct {

  def apply[A, K](f: A => K)(implicit ser: Serializer[K]): SpillingDistinct[A, K] =
    new SpillingDistinct(ser, f)

}

private[akua] trait SpillingDistinctOps[Out, Mat] {

  type Repr[O] <: akka.stream.scaladsl.FlowOps[O, Mat] {
    type Repr[OO] <: SpillingDistinctOps.this.Repr[OO]
  }

  protected def self: Repr[Out]

  def spillingDistinctBy[A : Serializer](f: Out => A): Repr[Out] = self.via(SpillingDistinct(f))
  def spillingDistinct(implicit ser: Serializer[Out]): Repr[Out] = spillingDistinctBy(identity)

}

final class SourceSpillingDistinctOps[Out, Mat](override protected val self: Source[Out, Mat]) extends SpillingDistinctOps[Out, Mat] {
  override type Repr[O] = Source[O, Mat]
}

final class FlowSpillingDistinctOps[In, Out, Mat](override protected val self: Flow[In, Out, Mat]) extends SpillingDistinctOps[Out, Mat] {
  override type Repr[O] = Flow[In, O, Mat]
}

final class SubFlowSpillingDistinctOps[Out, Mat, F[+_], C](override val self: SubFlow[Out, Mat, F, C]) extends SpillingDistinctOps[Out, Mat] {
  override type Repr[O] = SubFlow[O, Mat, F, C]
}

trait ToSpillingDistinctOps {
  implicit def toSourceSpillingDistinctOps[Out, Mat](source: Source[Out, Mat]): SourceSpillingDistinctOps[Out, Mat] = new SourceSpillingDistinctOps(source)
  implicit def toFlowSpillingDistinctOps[In, Out, Mat](flow: Flow[In, Out, Mat]): FlowSpillingDistinctOps[In, Out, Mat] = new FlowSpillingDistinctOps(flow)
  implicit def toSubFlowSpillingDistinctOps[Out, Mat, F[+_], C](sub: SubFlow[Out, Mat, F, C]): SubFlowSpillingDistinctOps[Out, Mat, F, C] = new SubFlowSpillingDistinctOps(sub)
}
