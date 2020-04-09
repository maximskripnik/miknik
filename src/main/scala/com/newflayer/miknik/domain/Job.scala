package com.newflayer.miknik.domain

import java.time.Instant

case class Job(
  id: String,
  resources: Resources,
  dockerImage: String,
  cmd: List[String],
  env: Map[String, String],
  status: JobStatus,
  error: Option[String],
  created: Instant,
  updated: Instant
)
