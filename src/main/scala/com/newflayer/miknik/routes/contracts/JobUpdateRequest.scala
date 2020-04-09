package com.newflayer.miknik.routes.contracts

import com.newflayer.miknik.domain.JobStatus
import com.newflayer.miknik.routes.contracts.JobResponse._

import io.circe.Decoder
import io.circe.generic.semiauto._

case class JobUpdateRequest(
  status: Option[JobStatus]
)

object JobUpdateRequest {
  implicit val decoder: Decoder[JobUpdateRequest] = deriveDecoder[JobUpdateRequest]
}
