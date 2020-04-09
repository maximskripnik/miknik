package com.newflayer.miknik.routes.contracts

import com.newflayer.miknik.domain.{ Job, JobStatus }
import com.newflayer.miknik.utils.circe.EnumHelper

import java.time.Instant

import io.circe._
import io.circe.generic.semiauto._
import io.scalaland.chimney.dsl._

case class JobResponse(
  id: String,
  resources: Resources,
  dockerImage: String,
  cmd: List[String],
  env: Map[String, String],
  status: JobStatus,
  error: Option[String],
  created: Instant,
  updated: Instant,
  completed: Option[Instant]
)

object JobResponse {

  import JobStatus._

  implicit val jobStatusCodec: Codec[JobStatus] = EnumHelper.buildCodec(
    {
      case Pending => "pending"
      case Running => "running"
      case Completed => "completed"
      case Failed => "failed"
      case Canceled => "canceled"
    }, {
      case "pending" => Pending
      case "running" => Running
      case "completed" => Completed
      case "failed" => Failed
      case "canceled" => Canceled
    }
  )

  implicit val encoder: Encoder[JobResponse] = deriveEncoder

  def apply(job: Job): JobResponse = job.into[JobResponse].transform

}
