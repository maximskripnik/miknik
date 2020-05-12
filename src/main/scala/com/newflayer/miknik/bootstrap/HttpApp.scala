package com.newflayer.miknik.bootstrap

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.ChildFailed
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.Terminated
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import com.typesafe.config.ConfigFactory

object HttpApp {

  def main(args: Array[String]): Unit = {
    ActorSystem(HttpServer("0.0.0.0", 5150), "miknik")
  }

}

object HttpServer {
  sealed trait Message
  case class ServicesInstantiated(services: Services) extends Message
  case class Started(binding: ServerBinding) extends Message

  def apply(host: String, port: Int): Behavior[Message] = Behaviors.setup { context =>
    implicit val ec = context.executionContext

    val servicesActor = context.spawnAnonymous(ServiceInstantiator(ConfigFactory.load()))
    context.watch(servicesActor)

    val serviceInstantiator = new ServiceInstantiator(servicesActor)(context.system.scheduler)

    context.pipeToSelf(serviceInstantiator.getServices(15.seconds)) {
      case Success(services) => ServicesInstantiated(services)
      case Failure(ex) => throw new RuntimeException("Failed to instantiate services", ex)
    }

    waitingForServices(host, port)
  }

  def waitingForServices(host: String, port: Int): Behavior[Message] =
    Behaviors
      .receivePartial[Message] {
        case (context, ServicesInstantiated(services)) =>
          implicit val actorSystem = context.system.toClassic
          val routesInstantiator = new RoutesInstantiator(services)

          context.pipeToSelf(Http().bindAndHandle(routesInstantiator.combineRoutes(), host, port)) {
            case Success(binding) => Started(binding)
            case Failure(ex) => throw new RuntimeException("Server failed to start", ex)
          }

          starting()
      }
      .receiveSignal {
        case (_, ChildFailed(_, ex)) => throw ex
      }

  def starting(): Behavior[Message] =
    Behaviors.receivePartial {
      case (context, Started(binding)) =>
        context.log.info(
          "Server online at http://{}:{}/",
          binding.localAddress.getHostString,
          binding.localAddress.getPort
        )
        Behaviors.ignore
    }
}
