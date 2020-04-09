package com.newflayer.miknik.bootstrap

import com.newflayer.miknik.services.JobService

import scala.concurrent.ExecutionContext

class ServiceInstantiator(implicit ec: ExecutionContext) {
  val jobService = new JobService()
}
