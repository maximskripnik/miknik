package com.newflayer.miknik.core

import scala.jdk.CollectionConverters._

import org.apache.mesos.v1.Protos.AgentID
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.Offer
import org.apache.mesos.v1.Protos.OfferID
import org.apache.mesos.v1.Protos.Resource
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskState
import org.apache.mesos.v1.Protos.TaskStatus
import org.apache.mesos.v1.Protos.Value
import org.apache.mesos.v1.scheduler.Protos.Event.Offers
import org.apache.mesos.v1.scheduler.Protos.Event.Update
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen._

trait MesosProtoGenerators {

  implicit val arbAgentID: Arbitrary[AgentID] =
    fromAlphaNumStr(AgentID.newBuilder().setValue(_).build())

  implicit val arbFrameworkID: Arbitrary[FrameworkID] =
    fromAlphaNumStr(FrameworkID.newBuilder().setValue(_).build())

  implicit val arbTaskID: Arbitrary[TaskID] =
    fromAlphaNumStr(TaskID.newBuilder().setValue(_).build())

  implicit val arbOfferID: Arbitrary[OfferID] =
    fromAlphaNumStr(OfferID.newBuilder().setValue(_).build())

  implicit val arbOffer: Arbitrary[Offer] = Arbitrary {
    for {
      offerId <- arbitrary[OfferID]
      frameworkId <- arbitrary[FrameworkID]
      agentId <- arbitrary[AgentID]
      hostname <- alphaNumStr
      cpus <- choose(0.1, 32.0)
      mem <- choose(1, 2 * 1024 * 1024) // 1MB - 2TB
      disk <- choose(1024, 100 * 1024 * 2014) // 1GB - 100TB
    } yield Offer
      .newBuilder()
      .setId(offerId)
      .setHostname(hostname)
      .setAgentId(agentId)
      .setFrameworkId(frameworkId)
      .addAllResources(
        List(
          buildScalarResource("cpus", cpus),
          buildScalarResource("mem", mem),
          buildScalarResource("disk", disk)
        ).asJava
      )
      .build()
  }

  implicit val arbTaskState: Arbitrary[TaskState] = Arbitrary {
    oneOf(TaskState.values.toIndexedSeq)
  }

  implicit val arbUpdate: Arbitrary[Update] = Arbitrary {
    for {
      taskId <- arbitrary[TaskID]
      taskState <- arbitrary[TaskState]
    } yield buildUpdate(taskId, taskState)
  }

  implicit val arbOffers: Arbitrary[Offers] = Arbitrary {
    listOfN(10, arbitrary[Offer]).map { offers =>
      Offers
        .newBuilder()
        .addAllOffers(offers.asJava)
        .build()
    }
  }

  protected def buildScalarResource(name: String, value: Double): Resource =
    Resource
      .newBuilder()
      .setName(name)
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(value))
      .build()

  protected def buildUpdate(taskId: TaskID, taskState: TaskState): Update =
    Update
      .newBuilder()
      .setStatus(
        TaskStatus
          .newBuilder()
          .setTaskId(taskId)
          .setState(taskState)
      )
      .build()

  private def fromAlphaNumStr[T](fromStr: String => T): Arbitrary[T] = Arbitrary {
    alphaNumStr.map(fromStr)
  }

}
