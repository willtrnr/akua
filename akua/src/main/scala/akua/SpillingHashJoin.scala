package akua

import scala.util.control.NonFatal

import java.util.{Map => JMap, Set => JSet}

import akka.NotUsed
import akka.stream.scaladsl.{Flow, GraphDSL, SubFlow, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Graph, Inlet, Outlet, SourceShape}

import org.mapdb.{DB, DBMaker, Serializer}

import akua.serializer.StdSerializers

private[akua] final class SpillingHashJoin[L, R, A](keySer: Serializer[A], valueSer: Serializer[R], extractKeyL: L => A, extractKeyR: R => A) extends GraphStage[JoinShape[L, R, JoinShape.Full[L, R]]] {

  import scala.collection.JavaConverters._

  val left: Inlet[L] = Inlet("SpillingHashJoin.left")
  val right: Inlet[R] = Inlet("SpillingHashJoin.right")
  val out: Outlet[JoinShape.Full[L, R]] = Outlet("SpillingHashJoin.out")

  override val shape: JoinShape[L, R, JoinShape.Full[L, R]] = JoinShape(left, right, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with OutHandler {

    private[this] var db: DB = _
    private[this] var table: JMap[A, Vector[R]] = _
    private[this] var unmatched: JSet[A] = _

    override def preStart(): Unit = {
      db = DBMaker.tempFileDB()
        .fileMmapEnableIfSupported()
        .fileMmapPreclearDisable()
        .cleanerHackEnable()
        .concurrencyDisable() // Akka Stream guarantees that callbacks are synchronized
        .make()

      try {
        table = db.hashMap("table")
          .keySerializer(keySer)
          .valueSerializer(StdSerializers.vectorSerializer(valueSer))
          .create()

        unmatched = db.hashSet("unmatched")
          .serializer(keySer)
          .create()
      } catch {
        case NonFatal(e) =>
          db.close()
          throw e
      }

      pull(right)
    }

    override def postStop(): Unit = {
      if (db ne null) db.close()
    }

    def drain(): Unit = {
      emitMultiple(
        out,
        unmatched.iterator.asScala.toStream
          .flatMap(table.get)
          .map(v => (None, Some(v))),
        () => completeStage()
      )
    }

    setHandler(left, new InHandler {

      override def onPush(): Unit = {
        val e = grab(left)
        val k = extractKeyL(e)
        if (table.containsKey(k)) {
          unmatched.remove(k)
          emitMultiple(out, table.get(k).map(v => (Some(e), Some(v))))
        } else {
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
        table.put(k, table.getOrDefault(k, Vector.empty) :+ e)
        unmatched.add(k)
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

object SpillingHashJoin {

  def apply[L, R, A](lf: L => A, rf: R => A)(implicit ks: Serializer[A], vs: Serializer[R]): SpillingHashJoin[L, R, A] =
    new SpillingHashJoin(ks, vs, lf, rf)

  def full[L, R : Serializer, A : Serializer](left: Source[L, _], right: Source[R, _])(lf: L => A, rf: R => A): Source[JoinShape.Full[L, R], NotUsed] =
    Source.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val join = b.add(apply(lf, rf))
      left  ~> join.left
      right ~> join.right
      SourceShape(join.out)
    })

  def inner[L, R : Serializer, A : Serializer](left: Source[L, _], right: Source[R, _])(lf: L => A, rf: R => A): Source[JoinShape.Inner[L, R], NotUsed] =
    full(left, right)(lf, rf) collect { case (Some(l), Some(r)) => (l, r) }

  def left[L, R : Serializer, A : Serializer](left: Source[L, _], right: Source[R, _])(lf: L => A, rf: R => A): Source[JoinShape.Left[L, R], NotUsed] =
    full(left, right)(lf, rf) collect { case (Some(l), r) => (l, r) }

  def right[L, R : Serializer, A : Serializer](left: Source[L, _], right: Source[R, _])(lf: L => A, rf: R => A): Source[JoinShape.Right[L, R], NotUsed] =
    full(left, right)(lf, rf) collect { case (l, Some(r)) => (l, r) }

  def outer[L, R : Serializer, A : Serializer](left: Source[L, _], right: Source[R, _])(lf: L => A, rf: R => A): Source[JoinShape.Outer[L, R], NotUsed] =
    full(left, right)(lf, rf) collect {
      case (Some(l), None) => Left(l)
      case (None, Some(r)) => Right(r)
    }

}

private[akua] trait SpillingHashJoinOps[Out, Mat] {

  type Repr[O] <: akka.stream.scaladsl.FlowOps[O, Mat] {
    type Repr[OO] <: SpillingHashJoinOps.this.Repr[OO]
  }

  protected def self: Repr[Out]

  private[this] def fullJoinGraph[Out2 : Serializer, A : Serializer, M](right: Graph[SourceShape[Out2], M])(lf: Out => A, rf: Out2 => A): Graph[FlowShape[Out, JoinShape.Full[Out, Out2]], M] =
    GraphDSL.create(right) { implicit b => r =>
      import GraphDSL.Implicits._
      val join = b.add(SpillingHashJoin(lf, rf))
      r ~> join.right
      FlowShape(join.left, join.out)
    }

  def fullSpillingHashJoin[Out2 : Serializer, A : Serializer](right: Graph[SourceShape[Out2], _])(lf: Out => A, rf: Out2 => A): Repr[JoinShape.Full[Out, Out2]] =
    self.via(fullJoinGraph(right)(lf, rf))

  def innerSpillingHashJoin[Out2 : Serializer, A : Serializer](right: Graph[SourceShape[Out2], _])(lf: Out => A, rf: Out2 => A): Repr[JoinShape.Inner[Out, Out2]] =
    fullSpillingHashJoin(right)(lf, rf) collect { case (Some(l), Some(r)) => (l, r) }

  def leftSpillingHashJoin[Out2 : Serializer, A : Serializer](right: Graph[SourceShape[Out2], _])(lf: Out => A, rf: Out2 => A): Repr[JoinShape.Left[Out, Out2]] =
    fullSpillingHashJoin(right)(lf, rf) collect { case (Some(l), r) => (l, r) }

  def rightSpillingHashJoin[Out2 : Serializer, A : Serializer](right: Graph[SourceShape[Out2], _])(lf: Out => A, rf: Out2 => A): Repr[JoinShape.Right[Out, Out2]] =
    fullSpillingHashJoin(right)(lf, rf) collect { case (l, Some(r)) => (l, r) }

  def outerSpillingHashJoin[Out2 : Serializer, A : Serializer](right: Graph[SourceShape[Out2], _])(lf: Out => A, rf: Out2 => A): Repr[JoinShape.Outer[Out, Out2]] =
    fullSpillingHashJoin(right)(lf, rf) collect {
      case (Some(l), None) => Left(l)
      case (None, Some(r)) => Right(r)
    }

}

final class SourceSpillingHashJoinOps[Out, Mat](override protected val self: Source[Out, Mat]) extends SpillingHashJoinOps[Out, Mat] {
  override type Repr[O] = Source[O, Mat]
}

final class FlowSpillingHashJoinOps[In, Out, Mat](override protected val self: Flow[In, Out, Mat]) extends SpillingHashJoinOps[Out, Mat] {
  override type Repr[O] = Flow[In, O, Mat]
}

final class SubFlowSpillingHashJoinOps[Out, Mat, F[+_], C](override protected val self: SubFlow[Out, Mat, F, C]) extends SpillingHashJoinOps[Out, Mat] {
  override type Repr[O] = SubFlow[O, Mat, F, C]
}

trait ToSpillingHashJoinOps {
  implicit def toSourceSpillingHashJoinOps[Out, Mat](source: Source[Out, Mat]): SourceSpillingHashJoinOps[Out, Mat] = new SourceSpillingHashJoinOps(source)
  implicit def toFlowSpillingHashJoinOps[In, Out, Mat](flow: Flow[In, Out, Mat]): FlowSpillingHashJoinOps[In, Out, Mat] = new FlowSpillingHashJoinOps(flow)
  implicit def toSubFlowSpillingHashJoinOps[Out, Mat, F[+_], C](sub: SubFlow[Out, Mat, F, C]): SubFlowSpillingHashJoinOps[Out, Mat, F, C] = new SubFlowSpillingHashJoinOps(sub)
}
