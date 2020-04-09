package com.newflayer.routes

import com.newflayer.BaseSpec
import com.newflayer.routes.contracts.ListResponse

import akka.http.scaladsl.testkit.ScalatestRouteTest
import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport
import io.circe.Decoder
import io.circe.generic.semiauto._

trait BaseRoutesSpec extends BaseSpec with ScalatestRouteTest with ErrorAccumulatingCirceSupport {
  implicit def listResponseDecoder[T: Decoder]: Decoder[ListResponse[T]] = deriveDecoder[ListResponse[T]]
}
