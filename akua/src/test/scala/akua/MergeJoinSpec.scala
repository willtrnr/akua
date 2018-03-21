package akua

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit

import org.scalatest.{BeforeAndAfterAll, WordSpecLike}

class MergeJoinSpec extends TestKit(ActorSystem("MergeJoinSpec")) with WordSpecLike with BeforeAndAfterAll {

  implicit private[this] val mat = ActorMaterializer()

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "A merge join" should {

    "join based on sortedness of the inputs" in {
      MergeJoin.full(Source(List(1, 3, 4, 4, 6, 6, 7, 7, 8)), Source(List(2, 3, 3, 6, 7, 7, 9, 10, 11)))(identity, identity)
        .runWith(TestSink.probe[(Option[Int], Option[Int])])
        .request(Long.MaxValue)
        .expectNext((Some(1), None))
        .expectNext((None, Some(2)))
        .expectNext((Some(3), Some(3)))
        .expectNext((Some(3), Some(3)))
        .expectNext((Some(4), None))
        .expectNext((Some(6), Some(6)))
        .expectNext((Some(6), Some(6)))
        .expectNext((Some(7), Some(7)))
        .expectNext((Some(7), Some(7)))
        .expectNext((Some(7), Some(7)))
        .expectNext((Some(7), Some(7)))
        .expectNext((Some(8), None))
        .expectNext((None, Some(9)))
        .expectNext((None, Some(10)))
        .expectNext((None, Some(11)))
        .expectComplete()
    }

    "pass left through when right is empty" in {
      MergeJoin.full(Source(List(1, 2, 3)), Source.empty[Int])(identity, identity)
        .runWith(TestSink.probe[JoinShape.Full[Int, Int]])
        .request(Long.MaxValue)
        .expectNext((Some(1), None))
        .expectNext((Some(2), None))
        .expectNext((Some(3), None))
        .expectComplete()
    }

    "pass right through when left is empty" in {
      MergeJoin.full(Source.empty[Int], Source(List(1, 2, 3)))(identity, identity)
        .runWith(TestSink.probe[JoinShape.Full[Int, Int]])
        .request(Long.MaxValue)
        .expectNext((None, Some(1)))
        .expectNext((None, Some(2)))
        .expectNext((None, Some(3)))
        .expectComplete()
    }

    "output nothing when both inputs are empty" in {
      MergeJoin.full(Source.empty[Int], Source.empty[Int])(identity, identity)
        .runWith(TestSink.probe[JoinShape.Full[Int, Int]])
        .request(Long.MaxValue)
        .expectComplete()
    }

  }

}
