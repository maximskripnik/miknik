package com.newflayer.miknik.core

import com.newflayer.miknik.BaseSpec

import akka.actor.ExtendedActorSystem
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import cats.implicits._
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.Offer
import org.apache.mesos.v1.scheduler.Protos.Call
import org.mockito.DefaultAnswers

class MesosHttpGatewaySpec extends ScalaTestWithActorTestKit with BaseSpec with MesosProtoGenerators {

  trait Setup {
    val http = mock[HttpExt]
    val actorSystem = mock[ExtendedActorSystem](DefaultAnswers.ReturnsDeepStubs)
    http.system returns actorSystem
    val masterAddress = "mesos.example.com:4242"
    val mesosGateway = new MesosHttpGateway(
      masterAddress,
      http
    )
  }

  "MesosHttpGateway#makeSchedulerCall" should {
    "wrap mesos Call message and send the http request with it and with mesos stream id" in forAll {
      mesosStreamId: String =>
        new Setup {
          val response = HttpResponse(StatusCodes.OK)
          http.singleRequest(*, *, *, *) returnsF response
          whenReady(mesosGateway.makeSchedulerCall(mesosStreamId, Call.newBuilder()))(_ shouldBe response)
        }
    }
  }

  "MesosHttpGateway#makeAnonymousSchedulerCall" should {
    "wrap mesos Call message and send the http request with it" in new Setup {
      val response = HttpResponse(StatusCodes.OK)
      http.singleRequest(*, *, *, *) returnsF response
      whenReady(mesosGateway.makeAnonymousSchedulerCall(Call.newBuilder()))(_ shouldBe response)
    }
  }

  "MesosHttpGateway#declineOffers" should {
    "wrap mesos Decline message and send the http request with it" in forAll {
      (mesosStreamId: String, frameworkId: FrameworkID, offers: List[Offer]) =>
        new Setup {
          val response = HttpResponse(StatusCodes.OK)
          http.singleRequest(*, *, *, *) returnsF response
          whenReady(mesosGateway.declineOffers(mesosStreamId, frameworkId, offers, testKit.system.log))(
            _ shouldBe response
          )
        }
    }
  }

}
