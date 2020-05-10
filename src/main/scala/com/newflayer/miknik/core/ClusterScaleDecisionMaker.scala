package com.newflayer.miknik.core

import com.newflayer.miknik.domain.BusyNode
import com.newflayer.miknik.domain.ClusterChanges
import com.newflayer.miknik.domain.Job
import com.newflayer.miknik.domain.Node

trait ClusterScaleDecisionMaker {

  def decideClusterScale(
    queue: List[Job],
    busyNodes: List[BusyNode],
    unusedNodes: List[Node]
  ): Option[ClusterChanges]

}
