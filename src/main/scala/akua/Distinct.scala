package akua

import scala.collection.mutable

import akka.NotUsed
import akka.stream.scaladsl.{Flow, SubFlow, Source}

object Distinct {

  def apply[A, K](f: A => K): Flow[A, A, NotUsed] =
    Flow[A] statefulMapConcat { () =>
      val seen: mutable.Set[K] = mutable.Set.empty
      (a: A) => {
        val k = f(a)
        if (seen(k)) Nil else {
          seen += k
          a :: Nil
        }
      }
    }

}

private[akua] trait DistinctOps[Out, Mat] {
  type Repr[O] <: akka.stream.scaladsl.FlowOps[O, Mat] { type Repr[OO] = DistinctOps.this.Repr[OO] }

  protected def self: Repr[Out]

  def distinctBy[A](f: Out => A): Repr[Out] = self.via(Distinct(f))
  def distinct: Repr[Out] = distinctBy(identity)
}

final class SourceDistinctOps[Out, Mat](override protected val self: Source[Out, Mat]) extends DistinctOps[Out, Mat] {
  override type Repr[+O] = Source[O, Mat]
}

final class FlowDistinctOps[In, Out, Mat](override protected val self: Flow[In, Out, Mat]) extends DistinctOps[Out, Mat] {
  override type Repr[+O] = Flow[In, O, Mat]
}

final class SubFlowDistinctOps[Out, Mat, F[+_], C](override val self: SubFlow[Out, Mat, F, C]) extends DistinctOps[Out, Mat] {
  override type Repr[+O] = SubFlow[O, Mat, F, C]
}

trait ToDistinctOps {
  implicit def toSourceDistinctOps[Out, Mat](source: Source[Out, Mat]): SourceDistinctOps[Out, Mat] = new SourceDistinctOps(source)
  implicit def toFlowDistinctOps[In, Out, Mat](flow: Flow[In, Out, Mat]): FlowDistinctOps[In, Out, Mat] = new FlowDistinctOps(flow)
  implicit def toSubFlowDistinctOps[Out, Mat, F[+_], C](sub: SubFlow[Out, Mat, F, C]): SubFlowDistinctOps[Out, Mat, F, C] = new SubFlowDistinctOps(sub)
}
