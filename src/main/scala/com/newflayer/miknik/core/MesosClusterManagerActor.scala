package com.newflayer.miknik.core

import com.newflayer.miknik.core.MesosClusterManagerActor.AgentLaunchResult
import com.newflayer.miknik.domain.Node

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.stream.Materializer
import akka.stream.alpakka.recordio.scaladsl.RecordIOFraming
import akka.stream.scaladsl.Sink
import cats.data.NonEmptyList
import org.apache.mesos.v1.Protos.AgentID
import org.apache.mesos.v1.master.Protos.Call
import org.apache.mesos.v1.master.Protos.Event
import org.apache.mesos.v1.master.Protos.Event.AgentAdded

private class MesosClusterManagerActor(
  mesosMasterAddress: String,
  mesosWorkDir: Option[String],
  clusterResourceManager: ClusterResourceManager,
  context: ActorContext[MesosClusterManagerActor.Message]
)(implicit ec: ExecutionContext, materializer: Materializer) {

  import MesosClusterManagerActor._

  private def waitingForSubscription(): Behavior[Message] = Behaviors.receiveMessagePartial {
    case Subscribed(response) =>
      response.entity.dataBytes
        .via(RecordIOFraming.scanner())
        .to(
          Sink.foreach { byteString =>
            val event = Event.parseFrom(byteString.toArray[Byte])
            if (event.getType() == Event.Type.AGENT_ADDED) {
              context.self ! MesosAgentAdded(event.getAgentAdded())
            }
          }
        )
        .run()
      running(State(Map.empty))
    case SubscriptionFailed(error) =>
      throw error
  }

  private def running(state: State): Behavior[Message] = Behaviors.receiveMessagePartial {
    case LaunchAgent(node, replyTo) =>
      val request = AgentLaunchRequest(node, replyTo)
      clusterResourceManager.executeCommand(node, buildMesosAgentCommand(node)).onComplete {
        case Success(_) => () // this result is not usefull. We better wait for MesosAgentAdded event from Mesos itself
        case Failure(error) => context.self ! AgentLaunchResult.AgentLaunchFailed(node, error)
      }
      running(state.copy(pendingRequests = state.pendingRequests.updated(node.id, request)))
    case MesosAgentAdded(event) =>
      context.log.debug(s"Received agent added event from Mesos: '$event'")
      event.getAgent().getAgentInfo().getAttributesList().asScala.collectFirst {
        case attribute if attribute.getName == "node_id" =>
          attribute.getText().getValue().drop(3) // drop the surrogate 'id_' prefix
      } match {
        case Some(nodeId) =>
          state.pendingRequests.get(nodeId) match {
            case Some(request) =>
              request.replyTo ! AgentLaunchResult.AgentLaunched(request.node, event.getAgent().getAgentInfo().getId())
              running(state.copy(pendingRequests = state.pendingRequests.removed(nodeId)))
            case None =>
              context.log.warn(
                s"Weird agent created with node id attribute set not by Miknik it seems: ${event.getAgent()}"
              )
              Behaviors.same
          }
        case None =>
          Behaviors.same
      }
    case agentLaunchFailed @ AgentLaunchResult.AgentLaunchFailed(node, error) =>
      context.log.error(s"Failed to launch agent for the node $node. Error: '$error'")
      clusterResourceManager.deallocate(NonEmptyList.of(node.id))
      state.pendingRequests(node.id).replyTo ! agentLaunchFailed
      running(state.copy(pendingRequests = state.pendingRequests.removed(node.id)))
  }

  private def buildMesosAgentCommand(node: Node): List[String] =
    List(
      "nohup",
      "mesos-agent",
      s"--master=$mesosMasterAddress",
      s"--work_dir=${mesosWorkDir.getOrElse("/tmp/mesos")}",
      "--ip=0.0.0.0",
      s"--advertise_ip=${node.ip}",
      "--no-systemd_enable_support",
      "--no-hostname_lookup",
      "--containerizers=docker",
      s"--attributes='node_id:id_${node.id}'", // add the surrogate id_ prefix for possible integer ids so that they are not inferred as scalars
      "> cmd.out 2> cmd.err &"
    )

}

object MesosClusterManagerActor {

  sealed trait AgentLaunchResult
  object AgentLaunchResult {
    case class AgentLaunched(node: Node, agentId: AgentID) extends AgentLaunchResult
    case class AgentLaunchFailed(node: Node, error: Throwable) extends AgentLaunchResult with Message
  }

  private type NodeId = String

  sealed trait Message
  case class LaunchAgent(node: Node, replyTo: ActorRef[AgentLaunchResult]) extends Message

  private case class Subscribed(response: HttpResponse) extends Message
  private case class SubscriptionFailed(error: Throwable) extends Message
  private case class MesosAgentAdded(event: AgentAdded) extends Message

  private case class AgentLaunchRequest(node: Node, replyTo: ActorRef[AgentLaunchResult])
  private case class State(pendingRequests: Map[NodeId, AgentLaunchRequest])

  def apply(
    mesosMasterAddress: String,
    mesosGateway: MesosHttpGateway,
    mesosWorkDir: Option[String],
    clusterResourceManager: ClusterResourceManager
  )(implicit ec: ExecutionContext, materializer: Materializer): Behavior[Message] = Behaviors.setup { context =>
    context.pipeToSelf(mesosGateway.makeMasterCall(Call.newBuilder().setType(Call.Type.SUBSCRIBE))) {
      case Success(response) =>
        if (response.status == StatusCodes.OK) {
          Subscribed(response)
        } else {
          val body = response.entity match {
            case HttpEntity.Strict(_, data) => data.utf8String
            case nonStrict => nonStrict.toString()
          }
          SubscriptionFailed(
            new RuntimeException(s"Bad response code on Mesos master subscribe: '${response.status}'. Body: '$body'")
          )
        }
      case Failure(error) =>
        SubscriptionFailed(error)
    }
    new MesosClusterManagerActor(
      mesosMasterAddress,
      mesosWorkDir,
      clusterResourceManager,
      context
    ).waitingForSubscription()
  }

}
