package com.newflayer.miknik.core.strategies

import com.newflayer.miknik.BaseSpec
import com.newflayer.miknik.domain.BusyNode
import com.newflayer.miknik.domain.Job
import com.newflayer.miknik.domain.JobGenerators
import com.newflayer.miknik.domain.Node
import com.newflayer.miknik.domain.NodeGenerators

import scala.concurrent.duration._

import java.time.Instant

import org.scalacheck.Arbitrary._
import org.scalacheck.Gen

class MaxNJobsScaleDecisionMakerSpec extends BaseSpec with JobGenerators with NodeGenerators {

  "MaxNJobsScaleDecisionMaker#decideClusterScale" should {

    "decide no changes when jobs queue is not big enough and no unused nodes" in forAll(Gen.listOfN(9, arbitrary[Job])) {
      queue =>
        val decisionMaker = new MaxNJobsScaleDecisionMaker(maxJobs = 10, maxNodes = 10, maxNodeUnusedTime = 30.seconds)
        decisionMaker.decideClusterScale(queue = queue, List.empty, List.empty) shouldBe None
    }

    "decide to scale cluster up when queue is too large and node limit is not reached" in forAll(
      Gen.nonEmptyListOf(arbitrary[Job]),
      Gen.listOfN(5, arbitrary[BusyNode]),
      Gen.listOfN(5, arbitrary[Node])
    ) { (jobs, busyNodes, unusedNodes) =>
      whenever(jobs.size > 3) {
        val decisionMaker = new MaxNJobsScaleDecisionMaker(maxJobs = 3, maxNodes = 11, maxNodeUnusedTime = 30.seconds)
        val result = decisionMaker.decideClusterScale(jobs, busyNodes, unusedNodes)
        result shouldBe a[Some[_]]
        result.get.add should not be empty
      }
    }

    "decide to scale cluster down when some nodes were not used for too long" in forAll { node: Node =>
      val oldNode = node.copy(lastUsedAt = Instant.now().minusSeconds(100))
      val unusedNodes = List(oldNode)
      val decisionMaker = new MaxNJobsScaleDecisionMaker(maxJobs = 10, maxNodes = 10, maxNodeUnusedTime = 30.seconds)
      val result = decisionMaker.decideClusterScale(List.empty, List.empty, unusedNodes)
      result shouldBe a[Some[_]]
      result.get.remove should contain(oldNode)
    }
  }

}
