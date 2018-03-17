package akua

import akka.NotUsed
import akka.stream.scaladsl.{GraphDSL, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FanInShape2, Inlet, Outlet, SourceShape}

// FIXME: Since the current item is flushed out after a match it won't work when the same item is emited multiple times
private[akua] final class MergeJoin[L, R, A](extractKeyL: L => A, extractKeyR: R => A, ord: Ordering[A]) extends GraphStage[FanInShape2[L, R, (Option[L], Option[R])]] {

  val in0: Inlet[L] = Inlet("MergeJoin.in0")
  val in1: Inlet[R] = Inlet("MergeJoin.in1")
  val out: Outlet[(Option[L], Option[R])] = Outlet("MergeJoin.out")

  override val shape: FanInShape2[L, R, (Option[L], Option[R])] = new FanInShape2(in0, in1, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with OutHandler {

    private[this] var currentLeft: Option[(L, A)] = None
    private[this] var currentRight: Option[(R, A)] = None

    override def preStart(): Unit = {
      pull(in0)
      pull(in1)
    }

    private[this] def join(): Option[(Option[L], Option[R])] =
      for ((l, lk) <- currentLeft; (r, rk) <- currentRight) yield {
        if (ord.equiv(lk, rk)) {
          currentLeft = None
          currentRight = None
          (Some(l), Some(r))
        } else if (ord.lt(lk, rk)) {
          currentLeft = None
          (Some(l), None)
        } else {
          currentRight = None
          (None, Some(r))
        }
      }

    private[this] def drain(): Unit = {
      emitMultiple(
        out,
        List(
          join(),
          currentLeft.map(l => (Some(l._1), None)),
          currentRight.map(r => (None, Some(r._1)))
        ).flatten,
        () => {
          if (!isClosed(in0)) {
            setHandlers(in0, out, new InHandler with OutHandler {
              override def onPush(): Unit = push(out, (Some(grab(in0)), None))
              override def onPull(): Unit = if (!hasBeenPulled(in0)) pull(in0)
              override def onUpstreamFinish(): Unit = completeStage()
            })
          } else if (!isClosed(in1)) {
            setHandlers(in1, out, new InHandler with OutHandler {
              override def onPush(): Unit = push(out, (None, Some(grab(in1))))
              override def onPull(): Unit = if (!hasBeenPulled(in1)) pull(in1)
              override def onUpstreamFinish(): Unit = completeStage()
            })
          } else {
            completeStage()
          }
        }
      )
    }

    setHandler(in0, new InHandler {

      override def onPush(): Unit = {
        val l = grab(in0)
        val lk = extractKeyL(l)
        currentLeft = Some((l, lk))
        if (isAvailable(out)) onPull()
      }

      override def onUpstreamFinish(): Unit = {
        drain()
      }

    })

    setHandler(in1, new InHandler {

      override def onPush(): Unit = {
        val r = grab(in1)
        val rk = extractKeyR(r)
        currentRight = Some((r, rk))
        if (isAvailable(out)) onPull()
      }

      override def onUpstreamFinish(): Unit = {
        drain()
      }

    })

    override def onPull(): Unit = {
      for (p <- join()) {
        push(out, p)
        if (currentLeft.isEmpty) tryPull(in0)
        if (currentRight.isEmpty) tryPull(in1)
      }
    }

    setHandler(out, this)

  }

}

object MergeJoin {

  def apply[L, R, A](extractLeft: L => A, extractRight: R => A)(implicit ord: Ordering[A]): MergeJoin[L, R, A] =
    new MergeJoin(extractLeft, extractRight, ord)

  def apply[L, R, A](left: Source[L, _], fl: L => A, right: Source[R, _], fr: R => A)(implicit ord: Ordering[A]): Source[(Option[L], Option[R]), NotUsed] =
    Source.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val join = b.add(apply(fl, fr))

      left  ~> join.in0
      right ~> join.in1

      SourceShape(join.out)
    })

  def apply[A](left: Source[A, _], right: Source[A, _])(implicit ord: Ordering[A]): Source[(Option[A], Option[A]), NotUsed] =
    apply(left, identity[A], right, identity[A])

}
