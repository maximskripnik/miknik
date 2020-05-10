package com.newflayer.miknik.core

import com.newflayer.miknik.domain.Node
import com.newflayer.miknik.domain.Resources

import scala.concurrent.Future

import cats.data.NonEmptyList

trait ClusterResourceManager {

  def allocate(nodeResources: NonEmptyList[Resources]): Future[NonEmptyList[Node]]

  def executeCommand(node: Node, cmd: List[String]): Future[Unit]

  def deallocate(nodeIds: NonEmptyList[String]): Future[Unit]

}
