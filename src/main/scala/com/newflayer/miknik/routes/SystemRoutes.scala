package com.newflayer.miknik.routes

import akka.http.scaladsl.server.Directives._

class SystemRoutes extends Routes {

  def routes = path("healthcheck") {
    get {
      complete("")
    }
  }

}
