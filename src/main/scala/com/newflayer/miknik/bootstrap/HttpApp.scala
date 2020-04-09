package com.newflayer.miknik.bootstrap

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

    val serviceInstantiator = new ServiceInstantiator()
    val routesInstantiator = new RoutesInstantiator(serviceInstantiator)

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