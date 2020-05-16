package com.newflayer.miknik.services

import com.newflayer.miknik.core.WorkloadSupervisorActor
import com.newflayer.miknik.dao.JobDao
import com.newflayer.miknik.domain.Job
import com.newflayer.miknik.domain.JobStatus
import com.newflayer.miknik.domain.ListResult
import com.newflayer.miknik.domain.Resources
import com.newflayer.miknik.services.JobService.CancelError
import com.newflayer.miknik.services.JobService.DeleteError

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import java.time.Instant

import akka.actor.typed.ActorRef
import akka.actor.typed.Scheduler
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import cats.data.EitherT
import cats.implicits._

class JobService(
  dao: JobDao,
  workloadSupervisorActor: ActorRef[WorkloadSupervisorActor.Message]
)(
  implicit ec: ExecutionContext,
  scheduler: Scheduler,
  jobCancelTimeout: Timeout
) {

  def create(
    id: String,
    resources: Resources,
    dockerImage: String,
    cmd: Option[List[String]],
    env: Option[Map[String, String]]
  ): Future[Job] =
    for {
      existingJob <- dao.get(id)
      _ = if (existingJob.isDefined) throw new RuntimeException(s"Job with id '$id' already exists")
      now = Instant.now()
      job = Job(
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
      createdJob <- dao.create(job)
      _ = workloadSupervisorActor ! WorkloadSupervisorActor.ScheduleJob(createdJob)
    } yield createdJob

  def list(): Future[ListResult[Job]] =
    for {
      jobs <- dao.list()
      count <- dao.count()
    } yield ListResult(jobs, count)

  def cancel(id: String): Future[Either[CancelError, Unit]] = {
    val result = for {
      job <- EitherT.fromOptionF(dao.get(id), CancelError.NotFound(id))
      _ <- EitherT.cond[Future](
        List(JobStatus.Pending, JobStatus.Running).contains(job.status),
        (),
        CancelError.BadStatus(job.status)
      )
      reply <- EitherT.right[CancelError](workloadSupervisorActor.ask(WorkloadSupervisorActor.CancelJob(id, _)))
      _ = assert(reply)
    } yield ()

    result.value
  }

  def delete(id: String): Future[Either[DeleteError, Unit]] =
    dao.delete(id).map(Either.cond(_, (), DeleteError.NotFound(id)))

}

object JobService {

  sealed trait CancelError
  object CancelError {
    case class NotFound(id: String) extends CancelError
    case class BadStatus(status: JobStatus) extends CancelError
  }

  sealed trait DeleteError

  object DeleteError {
    case class NotFound(id: String) extends DeleteError
  }

}
