package com.newflayer.miknik.bootstrap

import com.newflayer.miknik.core.ClusterResourceManager
import com.newflayer.miknik.core.ClusterScaleDecisionMaker
import com.newflayer.miknik.core.MesosClusterManager
import com.newflayer.miknik.core.MesosFrameworkActor
import com.newflayer.miknik.core.MesosSchedulerGateway
import com.newflayer.miknik.core.WorkloadSupervisorActor
import com.newflayer.miknik.dao.JobDao
import com.newflayer.miknik.domain.BusyNode
import com.newflayer.miknik.domain.ClusterChanges
import com.newflayer.miknik.domain.Job
import com.newflayer.miknik.domain.Node
import com.newflayer.miknik.domain.Resources
import com.newflayer.miknik.services.JobService

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.Scheduler
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import akka.util.Timeout
import cats.data.NonEmptyList
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
    mesosMasterAddress: String,
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

    waitingForMesosInfo(List.empty, None, None)(Context(mesosMasterAddress, mesosGateway, mesosFrameworkActor, ec))
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

    val decisionMaker = new ClusterScaleDecisionMaker {
      def decideClusterScale(
        queue: List[Job],
        busyNodes: List[BusyNode],
        unusedNodes: List[Node]
      ): Option[ClusterChanges] = None
    }

    val clusterResourceManager = new ClusterResourceManager {
      def allocate(nodeResources: NonEmptyList[Resources]): Future[NonEmptyList[Node]] =
        Future.failed(new NotImplementedError)

      def executeCommand(node: Node, cmd: List[String]): Future[Unit] =
        Future.failed(new NotImplementedError)

      def deallocate(nodeIds: NonEmptyList[String]): Future[Unit] =
        Future.failed(new NotImplementedError)
    }

    val mesosClusterManager = new MesosClusterManager(
      ctx.mesosMasterAddress,
      None,
      clusterResourceManager
    )

    val jobDao = new JobDao()
    val workloadSupervisorActor = context.spawnAnonymous(
      WorkloadSupervisorActor(
        mesosStreamId,
        mesosFrameworkId,
        ctx.mesosFrameworkActor,
        ctx.mesosGateway,
        jobDao,
        decisionMaker,
        mesosClusterManager,
        30.seconds
      )
    )

    val services = new Services {
      val jobService =
        new JobService(jobDao, workloadSupervisorActor)(ec, context.system.scheduler, jobCancelTimeout = 1.minute)
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
