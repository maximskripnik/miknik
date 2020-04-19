package com.newflayer.miknik.services

import com.newflayer.miknik.core.WorkloadSupervisorActor
import com.newflayer.miknik.dao.JobDao
import com.newflayer.miknik.domain.Job
import com.newflayer.miknik.domain.JobStatus
import com.newflayer.miknik.domain.ListResult
import com.newflayer.miknik.domain.Resources
import com.newflayer.miknik.services.JobService.DeleteError
import com.newflayer.miknik.services.JobService.UpdateError

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import java.time.Instant

import akka.actor.typed.ActorRef
import cats.data.EitherT
import cats.implicits._

class JobService(
  dao: JobDao,
  workloadSupervisorActor: ActorRef[WorkloadSupervisorActor.Message]
)(
  implicit ec: ExecutionContext
) {

  def create(
    id: String,
    resources: Resources,
    dockerImage: String,
    cmd: Option[List[String]],
    env: Option[Map[String, String]]
  ): Future[Job] = {
    val now = Instant.now()
    val job = Job(
      id = id,
      resources = resources,
      dockerImage = dockerImage,
      cmd = cmd.getOrElse(List.empty),
      env = env.getOrElse(Map.empty),
      status = JobStatus.Pending,
      error = None,
      created = now,
      updated = now,
      completed = None
    )
    dao.create(job).map { job =>
      workloadSupervisorActor ! WorkloadSupervisorActor.ScheduleJob(job)
      job
    }
  }

  def list(): Future[ListResult[Job]] =
    for {
      jobs <- dao.list()
      count <- dao.count()
    } yield ListResult(jobs, count)

  def update(id: String, status: Option[JobStatus]): Future[Either[UpdateError, Job]] = {
    val result = for {
      updater <- EitherT.fromEither[Future] {
        status match {
          case None =>
            None.asRight[UpdateError]
          case Some(JobStatus.Canceled) =>
            Some((job: Job) => job.copy(updated = Instant.now(), status = JobStatus.Canceled)).asRight
          case Some(status) =>
            UpdateError.BadStatus(status).asLeft
        }
      }
      newJob <- EitherT.fromOptionF[Future, UpdateError, Job](
        updater match {
          case Some(updater) => dao.update(id, updater)
          case None => dao.get(id)
        },
        UpdateError.NotFound(id)
      )
    } yield newJob

    result.value
  }

  def delete(id: String): Future[Either[DeleteError, Unit]] =
    dao.delete(id).map(Either.cond(_, (), DeleteError.NotFound(id)))

}

object JobService {

  sealed trait UpdateError
  object UpdateError {
    case class NotFound(id: String) extends UpdateError
    case class BadStatus(status: JobStatus) extends UpdateError
  }

  sealed trait DeleteError

  object DeleteError {
    case class NotFound(id: String) extends DeleteError
  }

}
