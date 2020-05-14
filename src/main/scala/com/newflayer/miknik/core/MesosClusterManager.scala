package com.newflayer.miknik.core

import com.newflayer.miknik.core.MesosClusterManager.DeallocationParams
import com.newflayer.miknik.domain.Resources

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import MesosClusterManagerActor.AgentLaunchResult
import akka.actor.typed.ActorRef
import akka.actor.typed.Scheduler
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import cats.data.NonEmptyList
import cats.implicits._
import org.apache.mesos.v1.Protos.AgentID
import org.apache.mesos.v1.master.Protos.Call
import org.apache.mesos.v1.master.Protos.Call.MarkAgentGone

class MesosClusterManager(
  mesosGateway: MesosHttpGateway,
  clusterResourceManager: ClusterResourceManager,
  agentLaunchTimeout: Timeout,
  actor: ActorRef[MesosClusterManagerActor.Message]
)(implicit ec: ExecutionContext, scheduler: Scheduler) {

  implicit val timeout = agentLaunchTimeout

  def allocate(nodeResources: NonEmptyList[Resources]): Future[NonEmptyList[AgentLaunchResult]] =
    for {
      allocatedNodes <- clusterResourceManager.allocate(nodeResources)
      results <- allocatedNodes.traverse { node => actor.ask(MesosClusterManagerActor.LaunchAgent(node, _)) }
    } yield results

  def deallocate(deallocationParams: NonEmptyList[DeallocationParams]): Future[Unit] =
    for {
      _ <- deallocationParams.traverse { params =>
        mesosGateway.makeMasterCall(
          Call
            .newBuilder()
            .setType(Call.Type.MARK_AGENT_GONE)
            .setMarkAgentGone(MarkAgentGone.newBuilder().setAgentId(params.agentId))
        )
      }
      _ <- clusterResourceManager.deallocate(deallocationParams.map(_.nodeId))
    } yield ()

}

object MesosClusterManager {
  case class DeallocationParams(nodeId: String, agentId: AgentID)
}
