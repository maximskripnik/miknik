package com.newflayer.routes

class SystemRoutesSpec extends BaseRouteSpec {

  val routes = new SystemRoutes().routes

  "GET /healthcheck" should {
    "return 200 with empty body" in {
      Get("/healthcheck") ~> routes ~> check {
        responseAs[String] shouldBe empty
      }
    }
  }
}
