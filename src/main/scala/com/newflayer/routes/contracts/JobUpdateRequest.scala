package com.newflayer.routes.contracts

import com.newflayer.domain.JobStatus
import com.newflayer.routes.contracts.JobResponse._

import io.circe.Decoder
import io.circe.generic.semiauto._

case class JobUpdateRequest(
  status: Option[JobStatus]
)

object JobUpdateRequest {
  implicit val decoder: Decoder[JobUpdateRequest] = deriveDecoder[JobUpdateRequest]
}
