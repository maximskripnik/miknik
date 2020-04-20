package com.newflayer.miknik.core

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.stream.alpakka.recordio.scaladsl.RecordIOFraming
import akka.stream.scaladsl.Sink
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.FrameworkInfo
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.scheduler.Protos.Call.Subscribe
import org.apache.mesos.v1.scheduler.Protos.Event
import org.apache.mesos.v1.scheduler.Protos.Event.Offers
import org.apache.mesos.v1.scheduler.Protos.Event.Update

object MesosFrameworkActor {

  sealed trait Message
  case class SubscribeToMesosUpdates(replyTo: ActorRef[Update]) extends Message
  case class SubscribeToMesosOffers(replyTo: ActorRef[Offers]) extends Message
  private[core] case class FailedToReceiveMesosResponseException(
    cause: Throwable
  ) extends RuntimeException(cause)
    with Message
  private case class MesosResponded(response: HttpResponse) extends Message
  private case class Subscribed(response: HttpResponse) extends Message
  private case class MesosEvent(mesosEvent: Event) extends Message

  private case class State(
    mesosStreamIdSubscriber: ActorRef[String],
    mesosFrameworkIdSubscriber: ActorRef[FrameworkID],
    mesosUpdatesSubscriber: Option[ActorRef[Update]],
    mesosOffersSubscriber: Option[ActorRef[Offers]]
  )

  private[core] case class BadResponseFromMesosException(response: HttpResponse) extends RuntimeException
  private[core] case class UnexpectedMesosEventException(event: Event) extends RuntimeException

  def apply(
    mesosStreamIdSubscriber: ActorRef[String],
    mesosFrameworkIdSubscriber: ActorRef[FrameworkID],
    mesosGateway: MesosSchedulerGateway
  )(
    implicit ec: ExecutionContext
  ): Behavior[Message] =
    Behaviors.setup { context =>
      mesosGateway
        .makeAnonymousCall(
          Call
            .newBuilder()
            .setType(Call.Type.SUBSCRIBE)
            .setSubscribe(
              Subscribe
                .newBuilder()
                .setFrameworkInfo(
                  FrameworkInfo.newBuilder().setName("Miknik").setUser("miknik")
                )
            )
        )
        .onComplete {
          case Success(response) => context.self ! MesosResponded(response)
          case Failure(error) => context.self ! FailedToReceiveMesosResponseException(error)
        }
      waitingForMesosResponse(State(mesosStreamIdSubscriber, mesosFrameworkIdSubscriber, None, None), mesosGateway)
    }

  def waitingForMesosResponse(
    state: State,
    mesosGateway: MesosSchedulerGateway
  ): Behavior[Message] = Behaviors.setup { context =>
    Behaviors.receiveMessagePartial {
      case MesosResponded(response) =>
        if (response.status != StatusCodes.OK) {
          throw new BadResponseFromMesosException(response)
        } else {
          implicit val as = context.system
          val mesosStreamId = response.headers.collectFirst {
            case header if header.name == "Mesos-Stream-Id" => header.value
          }.get
          state.mesosStreamIdSubscriber ! mesosStreamId
          response.entity.dataBytes
            .via(RecordIOFraming.scanner())
            .to(
              Sink.foreach { byteString => context.self ! MesosEvent(Event.parseFrom(byteString.toArray[Byte])) }
            )
            .run()
          waitingForSubscribe(
            mesosStreamId,
            state,
            mesosGateway
          )
        }
      case msg: FailedToReceiveMesosResponseException =>
        throw msg
      case SubscribeToMesosUpdates(replyTo) =>
        waitingForMesosResponse(
          state.copy(mesosUpdatesSubscriber = Some(replyTo)),
          mesosGateway
        )
      case SubscribeToMesosOffers(replyTo) =>
        waitingForMesosResponse(
          state.copy(mesosOffersSubscriber = Some(replyTo)),
          mesosGateway
        )
    }

  }

  def waitingForSubscribe(
    mesosStreamId: String,
    state: State,
    mesosGateway: MesosSchedulerGateway
  ): Behavior[Message] = Behaviors.receiveMessagePartial {
    case SubscribeToMesosUpdates(replyTo) =>
      waitingForSubscribe(
        mesosStreamId,
        state.copy(mesosUpdatesSubscriber = Some(replyTo)),
        mesosGateway
      )
    case SubscribeToMesosOffers(replyTo) =>
      waitingForSubscribe(
        mesosStreamId,
        state.copy(mesosOffersSubscriber = Some(replyTo)),
        mesosGateway
      )
    case MesosEvent(mesosEvent) =>
      if (mesosEvent.getType == Event.Type.SUBSCRIBED) {
        val frameworkId = mesosEvent.getSubscribed.getFrameworkId
        state.mesosFrameworkIdSubscriber ! frameworkId
        handlingEvents(
          mesosStreamId,
          mesosEvent.getSubscribed.getFrameworkId,
          state,
          mesosGateway
        )
      } else {
        throw new UnexpectedMesosEventException(mesosEvent)
      }
  }

  def handlingEvents(
    mesosStreamId: String,
    frameworkId: FrameworkID,
    state: State,
    mesosGateway: MesosSchedulerGateway
  ): Behavior[Message] =
    Behaviors.setup { context =>
      Behaviors.receiveMessagePartial {
        case MesosEvent(mesosEvent) =>
          context.log.trace(s"Received mesos event: '${mesosEvent.getType}'")
          mesosEvent.getType match {
            case Event.Type.SUBSCRIBED =>
              throw new UnexpectedMesosEventException(mesosEvent)
            case Event.Type.OFFERS =>
              state.mesosOffersSubscriber match {
                case Some(subscriber) =>
                  subscriber ! mesosEvent.getOffers
                case None => // nobody has subscribed yet, so we have to decline offers ourselves
                  mesosGateway.declineOffers(
                    mesosStreamId,
                    frameworkId,
                    mesosEvent.getOffers.getOffersList.asScala.toList,
                    context.log
                  )
              }
            case Event.Type.UPDATE =>
              state.mesosUpdatesSubscriber.foreach(_ ! mesosEvent.getUpdate)
            case _ =>
              ()
          }
          Behaviors.same
        case SubscribeToMesosUpdates(replyTo) =>
          handlingEvents(
            mesosStreamId,
            frameworkId,
            state.copy(mesosUpdatesSubscriber = Some(replyTo)),
            mesosGateway
          )
        case SubscribeToMesosOffers(replyTo) =>
          handlingEvents(
            mesosStreamId,
            frameworkId,
            state.copy(mesosOffersSubscriber = Some(replyTo)),
            mesosGateway
          )
      }
    }

}
