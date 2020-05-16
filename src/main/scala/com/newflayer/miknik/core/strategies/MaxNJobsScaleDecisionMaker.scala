package com.newflayer.miknik.core.strategies

import com.newflayer.miknik.core.ClusterScaleDecisionMaker
import com.newflayer.miknik.domain.{ BusyNode, ClusterChanges, Job, Node }

import scala.collection.immutable.Nil
import scala.concurrent.duration.FiniteDuration

import java.time.Instant

class MaxNJobsScaleDecisionMaker(maxJobs: Int, maxNodes: Int, maxNodeUnusedTime: FiniteDuration)
  extends ClusterScaleDecisionMaker {

  override def decideClusterScale(
    queue: List[Job],
    busyNodes: List[BusyNode],
    unusedNodes: List[Node]
  ): Option[ClusterChanges] = {
    val totalNodes = busyNodes.size + unusedNodes.size
    val toAdd = queue match {
      case firstJob :: _ if (queue.size > maxJobs && totalNodes + 1 <= maxNodes) => List(firstJob.resources)
      case _ => List.empty
    }

    val now = Instant.now()
    val toRemove = unusedNodes.filter(_.lastUsedAt.plusSeconds(maxNodeUnusedTime.toSeconds).compareTo(now) < 1)

    (toAdd, toRemove) match {
      case (Nil, Nil) => None
      case _ => Some(ClusterChanges(toAdd, toRemove))
    }
  }

}
