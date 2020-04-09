package com.newflayer.miknik.routes.contracts

import io.circe.Decoder
import io.circe.generic.semiauto._

case class JobCreateRequest(
  id: String,
  resources: Resources,
  dockerImage: String,
  cmd: Option[List[String]],
  env: Option[Map[String, String]]
)

object JobCreateRequest {
  implicit val decoder: Decoder[JobCreateRequest] = deriveDecoder
}
