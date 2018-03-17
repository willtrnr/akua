package akua

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit

import org.scalatest.{BeforeAndAfterAll, WordSpecLike}

class SpillingHashJoinSpec extends TestKit(ActorSystem("SpillingHashJoinSpec")) with WordSpecLike with BeforeAndAfterAll {

  import StdSerializers._

  implicit private[this] val mat = ActorMaterializer()

  "A spilling hash join" should {

    "join based on a hashed key" in {
      SpillingHashJoin.full(Source(List(3, 7, 7, 1, 0, 9, 2, 3)), Source(List(8, 2, 2, 3, 7)))(identity, identity)
        .runWith(TestSink.probe[(Option[Int], Option[Int])])
        .request(Long.MaxValue)
        .expectNext((Some(3), Some(3)))
        .expectNext((Some(7), Some(7)))
        .expectNext((Some(7), Some(7)))
        .expectNext((Some(1), None))
        .expectNext((Some(0), None))
        .expectNext((Some(9), None))
        .expectNext((Some(2), Some(2)))
        .expectNext((Some(2), Some(2)))
        .expectNext((Some(3), Some(3)))
        .expectNext((None, Some(8)))
        .expectComplete()
    }

  }

}
