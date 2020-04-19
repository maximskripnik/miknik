package com.newflayer.miknik.core

import com.newflayer.miknik.dao.JobDao
import com.newflayer.miknik.domain.Job

import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.model.HttpResponse
import org.apache.mesos.v1.Protos.AgentID
import org.apache.mesos.v1.Protos.CommandInfo
import org.apache.mesos.v1.Protos.ContainerInfo
import org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo
import org.apache.mesos.v1.Protos.Environment
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.Offer
import org.apache.mesos.v1.Protos.Offer.Operation
import org.apache.mesos.v1.Protos.OfferID
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskInfo
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.scheduler.Protos.Call.Accept
import org.apache.mesos.v1.scheduler.Protos.Event.{ Offers => MesosOffers, Update => MesosUpdate }
import org.slf4j.Logger

object WorkloadSupervisorActor {

  sealed trait Message
  case class ScheduleJob(job: Job) extends Message
  case class Offers(mesosOffers: MesosOffers) extends Message
  case class Update(mesosUpdate: MesosUpdate) extends Message
  case class GetQueue(replyTo: ActorRef[List[Job]]) extends Message
  private case class OfferAccepted(jobId: String, taskId: TaskID, agentId: AgentID) extends Message
  private case class OfferAcceptFailed(offerId: OfferID, job: Job, error: Throwable) extends Message
  private case class WorkerIsDone(taskId: TaskID) extends Message

  private case class Context(
    mesosStreamId: String,
    frameworkId: FrameworkID,
    mesosGateway: MesosSchedulerGateway,
    jobDao: JobDao,
    ec: ExecutionContext
  )
  private case class State(queue: Queue[Job], workers: Map[TaskID, ActorRef[MesosJobActor.Message]])

  def apply(
    mesosStreamId: String,
    frameworkId: FrameworkID,
    mesosFrameworkActor: ActorRef[MesosFrameworkActor.Message],
    mesosGateway: MesosSchedulerGateway,
    jobDao: JobDao
  )(implicit ec: ExecutionContext): Behavior[Message] =
    Behaviors.setup { actorContext =>
      val context = Context(mesosStreamId, frameworkId, mesosGateway, jobDao, ec)

      mesosFrameworkActor ! MesosFrameworkActor.SubscribeToMesosOffers(actorContext.messageAdapter(Offers(_)))
      mesosFrameworkActor ! MesosFrameworkActor.SubscribeToMesosUpdates(actorContext.messageAdapter(Update(_)))

      running(State(Queue.empty, Map.empty))(context)
    }

  private def running(state: State)(implicit ctx: Context): Behavior[Message] =
    Behaviors
      .receive[Message] { (context, message) =>
        message match {
          case ScheduleJob(job) =>
            running(state.copy(queue = state.queue.enqueue(job)))
          case Offers(mesosOffers) =>
            val offers = mesosOffers.getOffersList.asScala.toList
            if (state.queue.isEmpty) {
              ctx.mesosGateway.declineOffers(ctx.mesosStreamId, ctx.frameworkId, offers, context.log)
              running(state)
            } else {
              val acceptableOffer = offers.find((offerIsGoodEnough(_, state.queue.front)))
              acceptableOffer match {
                case Some(offer) =>
                  val (job, newQueue) = state.queue.dequeue
                  val taskId = TaskID.newBuilder.setValue(job.id).build
                  acceptOffer(offer, job, taskId, context.log).onComplete {
                    case Success(_) => context.self ! OfferAccepted(job.id, taskId, offer.getAgentId)
                    case Failure(e) => context.self ! OfferAcceptFailed(offer.getId, job, e)
                  }(context.executionContext)
                  ctx.mesosGateway.declineOffers(
                    ctx.mesosStreamId,
                    ctx.frameworkId,
                    offers.filterNot(_.getId == offer.getId),
                    context.log
                  )
                  running(state.copy(queue = newQueue))
                case None =>
                  ctx.mesosGateway.declineOffers(ctx.mesosStreamId, ctx.frameworkId, offers, context.log)
                  Behaviors.same
              }
            }
          case Update(mesosUpdate) =>
            val taskId = mesosUpdate.getStatus.getTaskId
            state.workers.get(taskId) match {
              case Some(worker) =>
                worker ! MesosJobActor.Update(mesosUpdate)
              case None =>
                context.log.warn(s"Received an update for an unknown task: '$taskId'")
            }
            Behaviors.same
          case OfferAccepted(jobId, taskId, agentId) =>
            val worker = context.spawnAnonymous(
              MesosJobActor(
                jobId,
                ctx.jobDao,
                ctx.mesosStreamId,
                ctx.frameworkId,
                agentId,
                taskId,
                ctx.mesosGateway
              )(ctx.ec)
            )
            context.watchWith(worker, WorkerIsDone(taskId))
            running(state.copy(workers = state.workers.updated(taskId, worker)))
          case OfferAcceptFailed(offerId, job, error) =>
            context.log.error(
              s"Failed to accept offer '$offerId' for job ''. Putting that job back to top of the queue",
              error,
              job.id
            )
            running(state.copy(queue = state.queue.prepended(job)))
          case WorkerIsDone(taskId) =>
            running(state.copy(workers = state.workers.removed(taskId)))
          case GetQueue(replyTo) =>
            replyTo ! state.queue.toList
            Behaviors.same
        }
      }

  private def acceptOffer(
    offer: Offer,
    job: Job,
    taskId: TaskID,
    log: Logger
  )(
    implicit ctx: Context
  ): Future[HttpResponse] = {
    log.debug(s"Accepting offer '${offer.getId}'")

    val taskInfo = TaskInfo
      .newBuilder()
      .setName(job.id)
      .setTaskId(taskId)
      .setAgentId(offer.getAgentId())
      .addAllResources(offer.getResourcesList)
      .setCommand(
        CommandInfo
          .newBuilder()
          .setShell(false)
          .addAllArguments(job.cmd.asJava)
          .setEnvironment(
            Environment
              .newBuilder()
              .addAllVariables(
                job.env.toList.map {
                  case (name, value) =>
                    Environment.Variable
                      .newBuilder()
                      .setName(name)
                      .setValue(value)
                      .build
                }.asJava
              )
          )
      )
      .setContainer(
        ContainerInfo
          .newBuilder()
          .setType(ContainerInfo.Type.DOCKER)
          .setDocker(DockerInfo.newBuilder().setImage(job.dockerImage))
      )

    ctx.mesosGateway.makeCall(
      ctx.mesosStreamId,
      Call
        .newBuilder()
        .setType(Call.Type.ACCEPT)
        .setFrameworkId(ctx.frameworkId)
        .setAccept(
          Accept
            .newBuilder()
            .addOfferIds(offer.getId)
            .addOperations(
              Operation
                .newBuilder()
                .setType(Operation.Type.LAUNCH)
                .setLaunch(
                  Operation.Launch
                    .newBuilder()
                    .addTaskInfos(taskInfo)
                )
            )
        )
    )

  }

  private def offerIsGoodEnough(
    offer: Offer,
    job: Job
  ): Boolean = {
    val resources = offer.getResourcesList.asScala.toList
    resources.exists(resource => resource.getName == "cpus" && resource.getScalar.getValue >= job.resources.cpus) &&
    resources.exists(resource => resource.getName == "mem" && resource.getScalar.getValue >= job.resources.mem) &&
    resources.exists(resource => resource.getName == "disk" && resource.getScalar.getValue >= job.resources.disk)
  }

}
