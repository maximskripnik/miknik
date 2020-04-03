package com.newflayer.domain

sealed trait JobStatus

object JobStatus {
  case object Pending extends JobStatus
  case object Running extends JobStatus
  case object Completed extends JobStatus
  case object Failed extends JobStatus
  case object Canceled extends JobStatus
}
