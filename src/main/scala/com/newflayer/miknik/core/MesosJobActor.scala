package com.newflayer.miknik.core

import com.newflayer.miknik.dao.JobDao
import com.newflayer.miknik.domain.JobStatus

import scala.concurrent.ExecutionContext
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal

import java.time.Instant

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import org.apache.mesos.v1.Protos.AgentID
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskState._
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.scheduler.Protos.Call.Acknowledge
import org.apache.mesos.v1.scheduler.Protos.Call.Kill
import org.apache.mesos.v1.scheduler.Protos.Event.{ Update => MesosUpdate }

object MesosJobActor {

  sealed trait Message
  case class Update(mesosUpdate: MesosUpdate) extends Message
  case class Cancel(replyTo: ActorRef[Unit]) extends Message
  private object KillCallSuccess extends Message
  private case class KillCallFailure(error: Throwable) extends Message

  private[core] case class MessageHandlingException(message: Message, cause: Throwable) extends RuntimeException

  def apply(
    jobId: String,
    jobDao: JobDao,
    mesosStreamId: String,
    frameworkId: FrameworkID,
    agentId: AgentID,
    taskId: TaskID,
    mesosGateway: MesosHttpGateway
  )(implicit ec: ExecutionContext): Behavior[Message] =
    watchingTask(
      jobId,
      cancelMessage = None,
      jobDao,
      mesosStreamId,
      frameworkId,
      agentId,
      taskId,
      mesosGateway
    )

  def watchingTask(
    jobId: String,
    cancelMessage: Option[Cancel],
    jobDao: JobDao,
    mesosStreamId: String,
    frameworkId: FrameworkID,
    agentId: AgentID,
    taskId: TaskID,
    mesosGateway: MesosHttpGateway
  )(implicit ec: ExecutionContext): Behavior[Message] =
    Behaviors.receive[Message] {
      case (context, message) =>
        try {
          message match {
            case Update(mesosUpdate) =>
              val mesosStatus = mesosUpdate.getStatus
              assert(mesosStatus.getTaskId == taskId)
              context.log.debug("Received task update from mesos: '{}' for job '{}'", mesosUpdate, jobId)
              val newBehaviour: Behavior[Message] = mesosStatus.getState match {
                case TASK_STAGING | TASK_STARTING => // we haven't really started yet
                  Behaviors.same
                case TASK_RUNNING =>
                  context.log.debug("Job '{}' has started running", jobId)
                  jobDao.update(jobId, status = Some(JobStatus.Running))
                  Behaviors.same
                case TASK_FINISHED =>
                  context.log.debug("Job '{}' has finished", jobId)
                  jobDao.update(jobId, status = Some(JobStatus.Completed), completed = Some(Instant.now()))
                  Behaviors.stopped
                case status @ (
                      TASK_UNREACHABLE | TASK_LOST | TASK_UNKNOWN
                    ) => // this is some weird status, but task might still be running, so keep the same behavior
                  context.log.warn("Weird status '{}' for job '{}'. Continuing watching the job", status, jobId)
                  Behaviors.same
                case TASK_KILLED if cancelMessage.isDefined =>
                  context.log.debug("Job '{}' has been cancelled", jobId)
                  jobDao.update(jobId, status = Some(JobStatus.Canceled))
                  cancelMessage.get.replyTo ! ()
                  Behaviors.stopped
                case TASK_ERROR | TASK_GONE_BY_OPERATOR | TASK_DROPPED | TASK_GONE | TASK_FAILED | TASK_KILLED =>
                  context.log.debug("Job '{}' has failed", jobId)
                  jobDao.update(jobId, status = Some(JobStatus.Failed), error = Option(mesosStatus.getMessage))
                  Behaviors.stopped
                case unknown @ TASK_KILLING =>
                  throw new IllegalArgumentException(s"Unexpected status: '$unknown'")
              }
              if (mesosStatus.hasUuid) {
                mesosGateway.makeSchedulerCall(
                  mesosStreamId,
                  Call
                    .newBuilder()
                    .setType(Call.Type.ACKNOWLEDGE)
                    .setFrameworkId(frameworkId)
                    .setAcknowledge(
                      Acknowledge
                        .newBuilder()
                        .setAgentId(agentId)
                        .setTaskId(taskId)
                        .setUuid(mesosStatus.getUuid)
                    )
                )
              }
              newBehaviour
            case cancel: Cancel =>
              if (cancelMessage.isEmpty) {
                mesosGateway
                  .makeSchedulerCall(
                    mesosStreamId,
                    Call
                      .newBuilder()
                      .setType(Call.Type.KILL)
                      .setFrameworkId(frameworkId)
                      .setKill(
                        Kill
                          .newBuilder()
                          .setAgentId(agentId)
                          .setTaskId(taskId)
                      )
                  )
                  .onComplete {
                    case Success(_) => context.self ! KillCallSuccess
                    case Failure(error) => context.self ! KillCallFailure(error)
                  }
                watchingTask(
                  jobId,
                  cancelMessage = Some(cancel),
                  jobDao,
                  mesosStreamId,
                  frameworkId,
                  agentId,
                  taskId,
                  mesosGateway
                )
              } else {
                Behaviors.same
              }
            case KillCallFailure(error) =>
              throw new RuntimeException(s"Failed to receive response for HTTP KILL request to Mesos. Error: $error")
            case KillCallSuccess =>
              Behaviors.same
          }
        } catch {
          case NonFatal(ex) =>
            jobDao.update(
              jobId,
              error = Some("Failure on Miknik's side. Report a bug if you see this"),
              status = Some(JobStatus.Failed)
            )
            throw new MessageHandlingException(message, ex)
        }
    }

}
