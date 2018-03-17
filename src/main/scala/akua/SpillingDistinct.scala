package akua

import scala.util.control.NonFatal

import java.util.{Set => JSet}

import akka.NotUsed
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

  def apply[A](ser: Serializer[A]): Flow[A, A, NotUsed] =
    Flow.fromGraph(new SpillingDistinct(ser, identity[A]))

  def apply[A, K](ser: Serializer[K], f: A => K): Flow[A, A, NotUsed] =
    Flow.fromGraph(new SpillingDistinct(ser, f))

}

final class SourceSpillingDistinct[Out, Mat](self: Source[Out, Mat]) {
  def spillingDistinct(implicit ser: Serializer[Out]): Source[Out, Mat] = self.via(SpillingDistinct(ser))
  def spillingDistinctBy[A](f: Out => A)(implicit ser: Serializer[A]): Source[Out, Mat] = self.via(SpillingDistinct(ser, f))
}

trait ToSourceSpillingDistinct {
  implicit def toSourceSpillingDistinct[Out, Mat](source: Source[Out, Mat]): SourceSpillingDistinct[Out, Mat] = new SourceSpillingDistinct(source)
}

final class FlowSpillingDistinct[In, Out, Mat](self: Flow[In, Out, Mat]) {
  def spillingDistinct(implicit ser: Serializer[Out]): Flow[In, Out, Mat] = self.via(SpillingDistinct(ser))
  def spillingDistinctBy[A](f: Out => A)(implicit ser: Serializer[A]): Flow[In, Out, Mat] = self.via(SpillingDistinct(ser, f))
}

trait ToFlowSpillingDistinct {
  implicit def toFlowSpillingDistinct[In, Out, Mat](flow: Flow[In, Out, Mat]): FlowSpillingDistinct[In, Out, Mat] = new FlowSpillingDistinct(flow)
}

final class SubFlowSpillingDistinct[Out, Mat, F[+_], C](self: SubFlow[Out, Mat, F, C]) {
  def spillingDistinct(implicit ser: Serializer[Out]): SubFlow[Out, Mat, F, C] = self.via(SpillingDistinct(ser))
  def spillingDistinctBy[A](f: Out => A)(implicit ser: Serializer[A]): SubFlow[Out, Mat, F, C] = self.via(SpillingDistinct(ser, f))
}

trait ToSubFlowSpillingDistinct {
  implicit def toSubFlowSpillingDistinct[Out, Mat, F[+_], C](flow: SubFlow[Out, Mat, F, C]): SubFlowSpillingDistinct[Out, Mat, F, C] = new SubFlowSpillingDistinct(flow)
}
