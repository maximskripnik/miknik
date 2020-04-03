package com.newflayer.routes

import com.newflayer.BaseSpec

import akka.http.scaladsl.testkit.ScalatestRouteTest
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport

trait BaseRoutesSpec extends BaseSpec with ScalatestRouteTest with ErrorAccumulatingCirceSupport
