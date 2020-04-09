package com.newflayer.miknik.bootstrap

import com.newflayer.miknik.routes._

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

class RoutesInstantiator(services: ServiceInstantiator) {

  val systemRoutes = new SystemRoutes()
  val jobRoutes = new JobRoutes(services.jobService)

  def combineRoutes(): Route =
    List(
      systemRoutes,
      jobRoutes
    ).foldLeft[Route](reject)(_ ~ _.routes)

}
