package com.newflayer.miknik.routes

import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport

trait Routes extends ErrorAccumulatingCirceSupport {

  def routes: Route

}
