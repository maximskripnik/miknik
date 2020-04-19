package com.newflayer.miknik.bootstrap

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding

object HttpApp {

  def main(args: Array[String]): Unit = {
    ActorSystem(HttpServer("0.0.0.0", 5150), "miknik")
  }

}

object HttpServer {
  sealed trait Message
  case class Started(binding: ServerBinding) extends Message

  def apply(host: String, port: Int): Behavior[Message] = Behaviors.setup { context =>
    implicit val actorSystem = context.system.toClassic
    implicit val ec = context.executionContext

    val mesosMasterUrl: String = ???

    val servicesActor = context.spawnAnonymous(ServiceInstantiator(mesosMasterUrl))
    val serviceInstantiator = new ServiceInstantiator(servicesActor)(context.system.scheduler)
    val services = Await.result(serviceInstantiator.getServices(2.minutes), Duration.Inf)

    val routesInstantiator = new RoutesInstantiator(services)

    context.pipeToSelf(Http().bindAndHandle(routesInstantiator.combineRoutes(), host, port)) {
      case Success(binding) => Started(binding)
      case Failure(ex) => throw new RuntimeException("Server failed to start", ex)
    }

    starting()
  }

  def starting(): Behavior[Message] = Behaviors.receive[Message] { (context, message) =>
    message match {
      case Started(binding) =>
        context.log.info(
          "Server online at http://{}:{}/",
          binding.localAddress.getHostString,
          binding.localAddress.getPort
        )
        Behaviors.ignore
    }
  }
}
