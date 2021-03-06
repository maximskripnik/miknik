package com.newflayer.miknik.bootstrap

import com.newflayer.miknik.core.MesosClusterManager
import com.newflayer.miknik.core.MesosClusterManagerActor
import com.newflayer.miknik.core.MesosFrameworkActor
import com.newflayer.miknik.core.MesosHttpGateway
import com.newflayer.miknik.core.WorkloadSupervisorInstantiator
import com.newflayer.miknik.core.providers.DigitalOceanResourceManager
import com.newflayer.miknik.core.strategies.MaxNJobsScaleDecisionMaker
import com.newflayer.miknik.dao.JobDao
import com.newflayer.miknik.dao.NodeDao
import com.newflayer.miknik.services.JobService
import com.newflayer.miknik.utils.DurationConverters

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import java.sql.Connection
import java.sql.DriverManager

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.Scheduler
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import akka.util.Timeout
import com.typesafe.config.Config
import org.apache.mesos.v1.Protos.FrameworkID
import org.sqlite.SQLiteConfig

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
    config: Config,
    mesosMasterAddress: String,
    mesosGateway: MesosHttpGateway,
    mesosFrameworkActor: ActorRef[MesosFrameworkActor.Message],
    ec: ExecutionContext
  )

  def apply(config: Config)(implicit ec: ExecutionContext): Behavior[Message] = Behaviors.setup { context =>
    implicit val actorSystem = context.system.toClassic
    val mesosMasterAddress = config.getString("mesos.master-address")
    val http = Http()
    val mesosGateway = new MesosHttpGateway(mesosMasterAddress, http)

    val mesosFrameworkActor = context.spawnAnonymous(
      MesosFrameworkActor(
        context.messageAdapter(MesosStreamId(_)),
        context.messageAdapter(MesosFrameworkId(_)),
        mesosGateway
      )
    )

    waitingForMesosInfo(List.empty, None, None)(
      Context(config, mesosMasterAddress, mesosGateway, mesosFrameworkActor, ec)
    )
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
    implicit val actorSystem = context.system.toClassic
    implicit val scheduler = context.system.scheduler

    val decisionMakerConf = ctx.config.getConfig("scale-strategy.max-n-jobs")
    val decisionMaker = new MaxNJobsScaleDecisionMaker(
      decisionMakerConf.getInt("max-jobs"),
      decisionMakerConf.getInt("max-nodes"),
      DurationConverters.toScalaDuration(decisionMakerConf.getDuration("max-node-unused-time"))
    )

    val clusterResourceManager =
      Await.result(DigitalOceanResourceManager(ctx.config.getConfig("resource-manager.digital-ocean")), Duration.Inf)

    val mesosClusterManagerActor = context.spawnAnonymous(
      MesosClusterManagerActor(
        ctx.mesosMasterAddress,
        ctx.mesosGateway,
        None,
        clusterResourceManager
      )
    )

    val dataDirectory = ctx.config.getString("data-directory")
    val sqliteConfig = new SQLiteConfig()
    sqliteConfig.enforceForeignKeys(true)
    val connection = DriverManager.getConnection(
      s"jdbc:sqlite:$dataDirectory/digital_ocean.db",
      sqliteConfig.toProperties()
    )
    connection.setAutoCommit(false)

    initializeSchema(connection)

    val jobDao = new JobDao(connection)
    val nodeDao = new NodeDao(connection)

    val mesosClusterManager = new MesosClusterManager(
      ctx.mesosGateway,
      nodeDao,
      clusterResourceManager,
      30.seconds,
      mesosClusterManagerActor
    )

    val workloadSupervisorInstantiator = new WorkloadSupervisorInstantiator(
      nodeDao,
      jobDao,
      ctx.mesosGateway,
      decisionMaker,
      mesosClusterManager,
      30.seconds
    )

    val workloadSupervisorBehaviour = Await.result(
      workloadSupervisorInstantiator.createBehaviour(mesosStreamId, mesosFrameworkId, ctx.mesosFrameworkActor),
      Duration.Inf
    )
    val workloadSupervisorActor = context.spawnAnonymous(workloadSupervisorBehaviour)

    val services = new Services {
      val jobService =
        new JobService(jobDao, workloadSupervisorActor)(ec, scheduler, jobCancelTimeout = 1.minute)
    }

    servicesSubscribers.foreach(_ ! services)
    Behaviors.receiveMessagePartial {
      case GetServices(replyTo) =>
        replyTo ! services
        Behaviors.same
    }
  }

  private def initializeSchema(connection: Connection) = {
    val nodeTableStmt =
      connection.prepareStatement("""
        CREATE TABLE IF NOT EXISTS node (
          id TEXT NOT NULL,
          agent_id TEXT NOT NULL,
          ip TEXT NOT NULL,
          cpus NUMERIC NOT NULL,
          mem INTEGER NOT NULL,
          disk INTEGER NOT NULL,
          created_at TEXT NOT NULL,
          last_used_at TEXT NOT NULL,
          CONSTRAINT node_PK PRIMARY KEY (id)
        )""")

    val jobTableStmt =
      connection.prepareStatement("""
        CREATE TABLE IF NOT EXISTS job (
          id TEXT NOT NULL,
          node_id TEXT,
          task_id TEXT,
          docker_image TEXT NOT NULL,
          cpus NUMERIC NOT NULL,
          mem INTEGER NOT NULL,
          disk INTEGER NOT NULL,
          cmd TEXT NOT NULL,
          env TEXT NOT NULL,
          status TEXT NOT NULL,
          error TEXT,
          created TEXT NOT NULL,
          updated TEXT NOT NULL,
          completed TEXT,
          CONSTRAINT job_PK PRIMARY KEY (id),
          CONSTRAINT job_node_FK FOREIGN KEY(node_id) REFERENCES node(id) ON DELETE SET NULL
        )""")

    nodeTableStmt.executeUpdate()
    jobTableStmt.executeUpdate()
  }

}

trait Services {
  val jobService: JobService
}
