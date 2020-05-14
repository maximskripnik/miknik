package com.newflayer.miknik.core

import com.newflayer.miknik.BaseSpec

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.model.ContentType
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cats.implicits._
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.scheduler.Protos.Event
import org.apache.mesos.v1.scheduler.Protos.Event.Offers
import org.apache.mesos.v1.scheduler.Protos.Event.Subscribed
import org.apache.mesos.v1.scheduler.Protos.Event.Update
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen

class MesosFrameworkActorSpec
  extends ScalaTestWithActorTestKit
  with BaseSpec
  with LogCapturing
  with MesosProtoGenerators {

  trait Setup {
    val mesosStreamIdSubscriberProbe = createTestProbe[String]()
    val mesosFrameworkIdSubscriberProbe = createTestProbe[FrameworkID]()
    val mesosGateway = mock[MesosHttpGateway]

    def spawnActor() = testKit.spawn(
      MesosFrameworkActor(
        mesosStreamIdSubscriberProbe.ref,
        mesosFrameworkIdSubscriberProbe.ref,
        mesosGateway
      )
    )
  }

  "MesosFrameworkActor" should {

    implicit val arbEvent: Arbitrary[Event] = Arbitrary {
      for {
        eventType <- Gen.oneOf(Event.Type.HEARTBEAT, Event.Type.OFFERS, Event.Type.UPDATE)
        builder = Event.newBuilder().setType(eventType)
        _ <- eventType match {
          case Event.Type.OFFERS =>
            arbitrary[Offers].map(builder.setOffers(_))
          case Event.Type.UPDATE =>
            arbitrary[Update].map(builder.setUpdate(_))
          case _ =>
            Gen.const(builder)
        }
      } yield builder.build
    }

    "reply with mesos params and mesos events to actors subscribed after mesos sent subscribe event" in forAll(
      arbitrary[String],
      arbitrary[FrameworkID],
      Gen.listOfN(10, arbitrary[Event])
    ) { (mesosStreamId: String, frameworkId: FrameworkID, events: List[Event]) =>
      new Setup {
        val (response, eventListener) = buildSubscribeResponse(mesosStreamId)
        mesosGateway.makeAnonymousSchedulerCall(*) returnsF response
        val actor = spawnActor()

        mesosStreamIdSubscriberProbe.expectMessage(mesosStreamId)

        eventListener ! buildSubscribedEvent(frameworkId)
        mesosFrameworkIdSubscriberProbe.expectMessage(frameworkId)

        val offersSubscriber = createTestProbe[Offers]()
        val updatesSubscriber = createTestProbe[Update]()

        actor ! MesosFrameworkActor.SubscribeToMesosOffers(offersSubscriber.ref)
        actor ! MesosFrameworkActor.SubscribeToMesosUpdates(updatesSubscriber.ref)

        events.foreach { event =>
          eventListener ! event
          event.getType() match {
            case Event.Type.OFFERS => offersSubscriber.expectMessage(1.minute, event.getOffers)
            case Event.Type.UPDATE => updatesSubscriber.expectMessage(1.minute, event.getUpdate)
            case _ => ()
          }
        }
      }
    }

    "reply with mesos params and mesos events to actors subscribed before mesos sent subscribe event (but after mesos sent http resposne)" in forAll(
      arbitrary[String],
      arbitrary[FrameworkID],
      Gen.listOfN(10, arbitrary[Event])
    ) { (mesosStreamId: String, frameworkId: FrameworkID, events: List[Event]) =>
      new Setup {
        val (response, eventListener) = buildSubscribeResponse(mesosStreamId)
        mesosGateway.makeAnonymousSchedulerCall(*) returnsF response
        val actor = spawnActor()

        mesosStreamIdSubscriberProbe.expectMessage(mesosStreamId)

        val offersSubscriber = createTestProbe[Offers]()
        val updatesSubscriber = createTestProbe[Update]()

        actor ! MesosFrameworkActor.SubscribeToMesosOffers(offersSubscriber.ref)
        actor ! MesosFrameworkActor.SubscribeToMesosUpdates(updatesSubscriber.ref)

        eventListener ! buildSubscribedEvent(frameworkId)
        mesosFrameworkIdSubscriberProbe.expectMessage(frameworkId)

        events.foreach { event =>
          eventListener ! event
          event.getType() match {
            case Event.Type.OFFERS => offersSubscriber.expectMessage(1.minute, event.getOffers)
            case Event.Type.UPDATE => updatesSubscriber.expectMessage(1.minute, event.getUpdate)
            case _ => ()
          }
        }
      }
    }

    "reply with mesos params and mesos events to actors subscribed before mesos sent http resposne" in forAll(
      arbitrary[String],
      arbitrary[FrameworkID],
      Gen.listOfN(10, arbitrary[Event])
    ) { (mesosStreamId: String, frameworkId: FrameworkID, events: List[Event]) =>
      new Setup {
        val (response, eventListener) = buildSubscribeResponse(mesosStreamId)
        val responseP = Promise[HttpResponse]()
        mesosGateway.makeAnonymousSchedulerCall(*) returns responseP.future
        val actor = spawnActor()

        val offersSubscriber = createTestProbe[Offers]()
        val updatesSubscriber = createTestProbe[Update]()

        actor ! MesosFrameworkActor.SubscribeToMesosOffers(offersSubscriber.ref)
        actor ! MesosFrameworkActor.SubscribeToMesosUpdates(updatesSubscriber.ref)

        responseP.success(response)
        mesosStreamIdSubscriberProbe.expectMessage(mesosStreamId)

        eventListener ! buildSubscribedEvent(frameworkId)
        mesosFrameworkIdSubscriberProbe.expectMessage(frameworkId)

        events.foreach { event =>
          eventListener ! event
          event.getType() match {
            case Event.Type.OFFERS => offersSubscriber.expectMessage(1.minute, event.getOffers)
            case Event.Type.UPDATE => updatesSubscriber.expectMessage(1.minute, event.getUpdate)
            case _ => ()
          }
        }
      }
    }

    "decline mesos offers if no actor has subscribed for them" in forAll {
      (mesosStreamId: String, frameworkId: FrameworkID, offers: Offers) =>
        new Setup {
          val (response, eventListener) = buildSubscribeResponse(mesosStreamId)
          mesosGateway.makeAnonymousSchedulerCall(*) returnsF response
          spawnActor()

          mesosStreamIdSubscriberProbe.expectMessage(mesosStreamId)

          eventListener ! buildSubscribedEvent(frameworkId)
          mesosFrameworkIdSubscriberProbe.expectMessage(frameworkId)

          mesosGateway.declineOffers(mesosStreamId, frameworkId, *, *) returnsF HttpResponse()
          eventListener ! Event.newBuilder().setType(Event.Type.OFFERS).setOffers(offers).build()
          mesosGateway.declineOffers(mesosStreamId, frameworkId, *, *) wasCalled (once within 1.second)
        }
    }

    "fail if could not receive http response" in new Setup {
      mesosGateway.makeAnonymousSchedulerCall(*) returns Future.failed(new RuntimeException("boom"))
      LoggingTestKit
        .error[MesosFrameworkActor.FailedToReceiveMesosResponseException]
        .expect(spawnActor())
    }

    "fail if http response was not 200" in new Setup {
      mesosGateway.makeAnonymousSchedulerCall(*) returnsF HttpResponse(StatusCodes.BadRequest)
      LoggingTestKit
        .error[MesosFrameworkActor.BadResponseFromMesosException]
        .expect(spawnActor())
    }

    "fail if first event from mesos was not subscribed" in forAll { mesosStreamId: String =>
      new Setup {
        val (response, eventListener) = buildSubscribeResponse(mesosStreamId)
        mesosGateway.makeAnonymousSchedulerCall(*) returnsF response
        eventListener ! Event.newBuilder().setType(Event.Type.HEARTBEAT).build()
        LoggingTestKit
          .error[MesosFrameworkActor.UnexpectedMesosEventException]
          .expect(spawnActor())
      }
    }

    "fail when received a subscribed event for the second time" in forAll {
      (mesosStreamId: String, frameworkId: FrameworkID) =>
        new Setup {
          val (response, eventListener) = buildSubscribeResponse(mesosStreamId)
          mesosGateway.makeAnonymousSchedulerCall(*) returnsF response
          val event = buildSubscribedEvent(frameworkId)
          eventListener ! event
          eventListener ! event
          LoggingTestKit
            .error[MesosFrameworkActor.UnexpectedMesosEventException]
            .expect(spawnActor())
        }
    }

  }

  private def buildSubscribeResponse(
    mesosStreamId: String
  ): (HttpResponse, ActorRef[Event]) = {
    val (eventListener, eventsSource) =
      Source.actorRef[Event](PartialFunction.empty, PartialFunction.empty, 100, OverflowStrategy.fail).preMaterialize()

    val entitySource = eventsSource.map { protobuf =>
      val bytes = protobuf.toByteArray
      ByteString(bytes.size.toString) ++ ByteString("\n") ++ ByteString(bytes) // build record IO event
    }

    val response = HttpResponse(
      headers = List(
        RawHeader("Mesos-Stream-Id", mesosStreamId)
      ),
      entity = HttpEntity(ContentType(MesosHttpGateway.protobufType), entitySource)
    )

    (response, eventListener.toTyped[Event])
  }

  private def buildSubscribedEvent(frameworkId: FrameworkID): Event =
    Event
      .newBuilder()
      .setType(Event.Type.SUBSCRIBED)
      .setSubscribed(
        Subscribed
          .newBuilder()
          .setFrameworkId(frameworkId)
      )
      .build()

}
