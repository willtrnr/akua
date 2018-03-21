package akua

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit

import org.scalatest.{BeforeAndAfterAll, WordSpecLike}

class FoldBySpec extends TestKit(ActorSystem("FoldBySpec")) with WordSpecLike with BeforeAndAfterAll {

  implicit private[this] val mat = ActorMaterializer()

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "A fold by" should {

    "fold over adjacent keys" in {
      Source(List(1, 1, 1, 2, 2, 3, 4))
        .via(FoldBy(identity[Int])(_ + _))
        .runWith(TestSink.probe[(Int, Int)])
        .request(Long.MaxValue)
        .expectNext((1, 3))
        .expectNext((2, 4))
        .expectNext((3, 3))
        .expectNext((4, 4))
        .expectComplete()
    }

  }

}
