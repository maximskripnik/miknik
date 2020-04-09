package com.newflayer.miknik.bootstrap

import com.newflayer.miknik.core.MesosFrameworkActor
import com.newflayer.miknik.core.MesosSchedulerGateway
import com.newflayer.miknik.core.WorkloadSupervisorActor
import com.newflayer.miknik.dao.JobDao
import com.newflayer.miknik.services.JobService

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.Scheduler
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import akka.util.Timeout
import org.apache.mesos.v1.Protos.FrameworkID

class ServiceInstantiator(serviceActor: ActorRef[ServiceInstantiator.Message])(
  implicit scheduler: Scheduler
) {
  def getServices(implicit timeout: Timeout): Future[Services] = serviceActor ? (ServiceInstantiator.GetServices(_))
}

object ServiceInstantiator {

  sealed trait Message
  case class GetServices(replyTo: ActorRef[Services]) extends Message
  private case class MesosStreamId(mesosStreamId: String) extends Message
  private case class MesosFrameworkId(mesosFrameworkId: FrameworkID) extends Message

  private case class Context(
    mesosGateway: MesosSchedulerGateway,
    mesosFrameworkActor: ActorRef[MesosFrameworkActor.Message],
    ec: ExecutionContext
  )

  def apply(mesosMasterAddress: String)(implicit ec: ExecutionContext): Behavior[Message] = Behaviors.setup { context =>
    implicit val actorSystem = context.system.toClassic
    val http = Http()
    val mesosGateway = new MesosSchedulerGateway(mesosMasterAddress, http)

    val mesosFrameworkActor = context.spawnAnonymous(
      MesosFrameworkActor(
        context.messageAdapter(MesosStreamId(_)),
        context.messageAdapter(MesosFrameworkId(_)),
        mesosGateway
      )
    )

    waitingForMesosInfo(List.empty, None, None)(Context(mesosGateway, mesosFrameworkActor, ec))
  }

  private def waitingForMesosInfo(
    servicesSubscribers: List[ActorRef[Services]],
    mesosStreamId: Option[String],
    mesosFrameworkId: Option[FrameworkID]
  )(implicit ctx: Context): Behavior[Message] = Behaviors.setup { context =>
    Behaviors.receiveMessage {
      case GetServices(replyTo) =>
        waitingForMesosInfo(
          servicesSubscribers = replyTo :: servicesSubscribers,
          mesosStreamId,
          mesosFrameworkId
        )
      case MesosStreamId(mesosStreamId) =>
        context.log.debug("Mesos stream id: '{}'", mesosStreamId)
        mesosFrameworkId match {
          case Some(mesosFrameworkId) =>
            ready(servicesSubscribers, mesosStreamId, mesosFrameworkId)
          case None =>
            waitingForMesosInfo(
              servicesSubscribers,
              mesosStreamId = Some(mesosStreamId),
              mesosFrameworkId = None
            )
        }
      case MesosFrameworkId(mesosFrameworkId) =>
        context.log.debug("Mesos framework id: '{}'", mesosFrameworkId)
        mesosStreamId match {
          case Some(mesosStreamId) =>
            ready(servicesSubscribers, mesosStreamId, mesosFrameworkId)
          case None =>
            waitingForMesosInfo(
              servicesSubscribers,
              mesosFrameworkId = Some(mesosFrameworkId),
              mesosStreamId = None
            )
        }
    }
  }

  private def ready(
    servicesSubscribers: List[ActorRef[Services]],
    mesosStreamId: String,
    mesosFrameworkId: FrameworkID
  )(implicit ctx: Context): Behavior[Message] = Behaviors.setup { context =>
    implicit val ec = ctx.ec

    val jobDao = new JobDao()
    val workloadSupervisorActor = context.spawnAnonymous(
      WorkloadSupervisorActor(
        mesosStreamId,
        mesosFrameworkId,
        ctx.mesosFrameworkActor,
        ctx.mesosGateway,
        jobDao
      )
    )

    val services = new Services {
      val jobService = new JobService(jobDao, workloadSupervisorActor)
    }

    servicesSubscribers.foreach(_ ! services)
    Behaviors.receiveMessagePartial {
      case GetServices(replyTo) =>
        replyTo ! services
        Behaviors.same
    }
  }

}

trait Services {
  val jobService: JobService
}
