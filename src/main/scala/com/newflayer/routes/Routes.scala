package com.newflayer.routes

import akka.http.scaladsl.server.Route

trait Routes {

  def routes: Route

}
