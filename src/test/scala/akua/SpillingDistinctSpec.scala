package akua

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit

import org.mapdb.Serializer
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}

class SpillingDistinctSpec extends TestKit(ActorSystem("SpillingDistinctSpec")) with WordSpecLike with BeforeAndAfterAll {

  implicit private[this] val mat = ActorMaterializer()

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "A spilling distinct" should {

    "filter distinct values" in {
      Source(List("aaa", "aab", "aba", "aaa", "baa", "aba", "aab", "aaa"))
        .via(SpillingDistinct(Serializer.STRING))
        .runWith(TestSink.probe[String])
        .request(Long.MaxValue)
        .expectNext("aaa")
        .expectNext("aab")
        .expectNext("aba")
        .expectNext("baa")
        .expectComplete()
    }

    "filter distinct values based on a key" in {
      Source(List("a", "aa", "b", "bbb", "cc", "cccc", "a"))
        .via(SpillingDistinct[String, Integer](Serializer.INTEGER, _.length))
        .runWith(TestSink.probe[String])
        .request(Long.MaxValue)
        .expectNext("a")
        .expectNext("aa")
        .expectNext("bbb")
        .expectNext("cccc")
        .expectComplete()
    }

  }

}
