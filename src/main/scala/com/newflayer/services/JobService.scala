package com.newflayer.services

import com.newflayer.domain.Job
import com.newflayer.domain.JobStatus
import com.newflayer.domain.ListResult
import com.newflayer.domain.Resources
import com.newflayer.services.JobService.DeleteError
import com.newflayer.services.JobService.UpdateError

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import java.time.Instant

import cats.data.EitherT
import cats.implicits._

class JobService(var jobs: Map[String, Job] = Map.empty)(implicit ec: ExecutionContext) {

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
      updated = now
    )
    jobs = jobs.updated(id, job)
    job
  }.pure[Future]

  def list(): Future[ListResult[Job]] = ListResult(jobs.values.toList, jobs.size).pure[Future]

  def update(id: String, status: Option[JobStatus]): Future[Either[UpdateError, Job]] = {
    val result = for {
      job <- EitherT.fromOption[Future](jobs.get(id), UpdateError.NotFound(id))
      updated <- EitherT.fromEither[Future] {
        status match {
          case None => job.updated.asRight[UpdateError]
          case Some(JobStatus.Canceled) => Instant.now().asRight[UpdateError]
          case Some(status) => UpdateError.BadStatus(status).asLeft
        }
      }
      newJob = job.copy(status = status.getOrElse(job.status), updated = updated)
      _ = jobs = jobs.updated(id, newJob)
    } yield newJob

    result.value
  }

  def delete(id: String): Future[Either[DeleteError, Unit]] = {
    val result = for {
      job <- EitherT.fromOption[Future](jobs.get(id), DeleteError.NotFound(id))
      _ = jobs = jobs.removed(id)
    } yield ()

    result.value
  }

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
