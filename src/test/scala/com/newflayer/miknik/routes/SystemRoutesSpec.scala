package com.newflayer.miknik.routes

class SystemRoutesSpec extends BaseRoutesSpec {

  val routes = new SystemRoutes().routes

  "GET /healthcheck" should {
    "return 200 with empty body" in {
      Get("/healthcheck") ~> routes ~> check {
        responseAs[String] shouldBe empty
      }
    }
  }
}
