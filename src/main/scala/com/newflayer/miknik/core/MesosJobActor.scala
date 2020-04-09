package com.newflayer.miknik.core

import com.newflayer.miknik.dao.JobDao
import com.newflayer.miknik.domain.JobStatus

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

import java.time.Instant

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import org.apache.mesos.v1.Protos.AgentID
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskState._
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.scheduler.Protos.Call.Acknowledge
import org.apache.mesos.v1.scheduler.Protos.Event.{ Update => MesosUpdate }

object MesosJobActor {

  sealed trait Message
  case class Update(mesosUpdate: MesosUpdate) extends Message

  def apply(
    jobId: String,
    jobDao: JobDao,
    mesosStreamId: String,
    frameworkId: FrameworkID,
    agentId: AgentID,
    taskId: TaskID,
    mesosGateway: MesosSchedulerGateway
  )(implicit ec: ExecutionContext): Behavior[Message] =
    Behaviors.receivePartial[Message] {
      case (context, Update(mesosUpdate)) =>
        try {
          val mesosStatus = mesosUpdate.getStatus
          assert(mesosStatus.getTaskId == taskId)
          context.log.debug("Received task update from mesos: '{}' for job '{}'", mesosUpdate, jobId)
          val newBehaviour: Behavior[Message] = mesosStatus.getState match {
            case TASK_STAGING | TASK_STARTING => // we haven't really started yet
              Behaviors.same
            case TASK_RUNNING =>
              context.log.debug("Job '{}' has started running", jobId)
              jobDao.update(jobId, _.copy(status = JobStatus.Running))
              Behaviors.same
            case TASK_FINISHED =>
              context.log.debug("Job '{}' has finished", jobId)
              jobDao.update(jobId, _.copy(status = JobStatus.Completed, completed = Some(Instant.now())))
              Behaviors.stopped
            case status @ (
                  TASK_UNREACHABLE | TASK_LOST | TASK_UNKNOWN
                ) => // this is some weird status, but task might still be running, so keep the same behavior
              context.log.warn("Weird status '{}' for job '{}'. Continuing watching the job", status, jobId)
              Behaviors.same
            case TASK_ERROR | TASK_GONE_BY_OPERATOR | TASK_DROPPED | TASK_KILLED | TASK_GONE | TASK_FAILED =>
              context.log.debug("Job '{}' has failed", jobId)
              jobDao.update(jobId, _.copy(status = JobStatus.Failed, error = Option(mesosStatus.getMessage)))
              Behaviors.stopped
            case unknown @ TASK_KILLING =>
              throw new RuntimeException(s"Unexpected status: '${unknown}'")
          }
          if (mesosStatus.hasUuid) {
            mesosGateway.makeCall(
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
        } catch {
          case NonFatal(ex) =>
            context.log.error("Error handling mesos update message. Job will be marked as failed", ex)
            jobDao.update(
              jobId,
              _.copy(
                error = Some("Failure on Miknik's side. Report a bug if you see this"),
                status = JobStatus.Failed
              )
            )
            throw ex
        }
    }

}
