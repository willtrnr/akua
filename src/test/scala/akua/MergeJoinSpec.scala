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

    // FIXME: Broken
    "join sorted input" ignore {
      MergeJoin.full(Source(List(1, 3, 4, 6, 8)), Source(List(2, 3, 6, 7, 9, 10, 11)))(identity, identity)
        .runWith(TestSink.probe[(Option[Int], Option[Int])])
        .request(Long.MaxValue)
        .expectNext((Some(1), None))
        .expectNext((None, Some(2)))
        .expectNext((Some(3), Some(3)))
        .expectNext((Some(4), None))
        .expectNext((Some(6), Some(6)))
        .expectNext((None, Some(7)))
        .expectNext((Some(8), None))
        .expectNext((None, Some(9)))
        .expectNext((None, Some(10)))
        .expectNext((None, Some(11)))
        .expectComplete()
    }

  }

}
