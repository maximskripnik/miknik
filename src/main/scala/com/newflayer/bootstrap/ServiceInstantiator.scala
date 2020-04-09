package com.newflayer.bootstrap

import com.newflayer.services.JobService

import scala.concurrent.ExecutionContext

class ServiceInstantiator(implicit ec: ExecutionContext) {
  val jobService = new JobService()
}
