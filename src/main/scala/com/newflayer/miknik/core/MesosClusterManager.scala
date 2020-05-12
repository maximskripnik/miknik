package com.newflayer.miknik.core

import com.newflayer.miknik.core.MesosClusterManager.AgentLaunchResult
import com.newflayer.miknik.domain.Node
import com.newflayer.miknik.domain.Resources

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import cats.data.NonEmptyList
import cats.implicits._

class MesosClusterManager(
  mesosMasterAddress: String,
  mesosWorkDir: Option[String],
  clusterResourceManager: ClusterResourceManager
)(implicit ec: ExecutionContext) {

  def allocate(nodeResources: NonEmptyList[Resources]): Future[NonEmptyList[AgentLaunchResult]] =
    for {
      allocatedNodes <- clusterResourceManager.allocate(nodeResources)
      results <- allocatedNodes.traverse { node =>
        clusterResourceManager
          .executeCommand(node, buildMesosAgentCommand(node))
          .redeem(
            AgentLaunchResult.AgentLaunchFailed(node, _),
            _ => AgentLaunchResult.AgentLaunched(node)
          ) // FIXME deallocate node if failed
      }
    } yield results

  def deallocate(nodeIds: NonEmptyList[String]): Future[Unit] =
    clusterResourceManager.deallocate(nodeIds)

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
      s"--attributes='node_id:${node.id}'",
      "> cmd.out 2> cmd.err &"
    )

}

object MesosClusterManager {
  sealed trait AgentLaunchResult
  object AgentLaunchResult {
    case class AgentLaunched(node: Node) extends AgentLaunchResult
    case class AgentLaunchFailed(node: Node, error: Throwable) extends AgentLaunchResult
  }
}
