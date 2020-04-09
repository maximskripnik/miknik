package com.newflayer.miknik.dao

import com.newflayer.miknik.domain.Job

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import cats.implicits._

class JobDao(var jobs: Map[String, Job] = Map.empty) {

  def create(job: Job)(implicit ec: ExecutionContext): Future[Job] = {
    jobs = jobs.updated(job.id, job)
    job.pure[Future]
  }

  def get(jobId: String)(implicit ec: ExecutionContext): Future[Option[Job]] =
    jobs.get(jobId).pure[Future]

  def list()(implicit ec: ExecutionContext): Future[List[Job]] =
    jobs.values.toList.pure[Future]

  def count()(implicit ec: ExecutionContext): Future[Long] =
    jobs.size.toLong.pure[Future]

  def update(jobId: String, updater: Job => Job)(implicit ec: ExecutionContext): Future[Option[Job]] =
    jobs.get(jobId) match {
      case Some(oldJob) =>
        val updatedJob = updater(oldJob)
        jobs = jobs.updated(jobId, updatedJob)
        Some(updatedJob).pure[Future]
      case None =>
        None.pure[Future]
    }

  def delete(jobId: String)(implicit ec: ExecutionContext): Future[Boolean] =
    jobs.get(jobId) match {
      case Some(_) =>
        jobs = jobs.removed(jobId)
        true.pure[Future]
      case None =>
        false.pure[Future]
    }

}
