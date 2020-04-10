package com.newflayer.miknik.core

import com.newflayer.miknik.BaseSpec
import com.newflayer.miknik.dao.JobDao

import java.util.UUID

import akka.actor.testkit.typed.scaladsl.BehaviorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.google.protobuf.ByteString
import org.apache.mesos.v1.Protos.AgentID
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskState
import org.apache.mesos.v1.Protos.TaskStatus
import org.apache.mesos.v1.scheduler.Protos.Event.Update
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen

class MesosJobActorSpec extends ScalaTestWithActorTestKit with BaseSpec with LogCapturing with MesosProtoGenerators {

  class Setup(
    val jobId: String,
    val mesosStreamId: String,
    val frameworkId: FrameworkID,
    val agentId: AgentID,
    val taskId: TaskID
  ) {
    val jobDao = mock[JobDao]
    val mesosGateway = mock[MesosSchedulerGateway]
    val actorTestKit = BehaviorTestKit(
      MesosJobActor(
        jobId = jobId,
        jobDao = jobDao,
        mesosStreamId = mesosStreamId,
        frameworkId = frameworkId,
        agentId = agentId,
        taskId = taskId,
        mesosGateway = mesosGateway
      )
    )
  }

  implicit val arbSetup: Arbitrary[Setup] = Arbitrary {
    for {
      jobId <- arbitrary[String]
      mesosStreamId <- arbitrary[String]
      frameworkId <- arbitrary[FrameworkID]
      agentId <- arbitrary[AgentID]
      taskId <- arbitrary[TaskID]
    } yield new Setup(jobId, mesosStreamId, frameworkId, agentId, taskId)
  }

  "MesosJobActor" should {

    "acknowledge TASK_STARTING or TASK_STAGING mesos update, but do nothing" in forAll(
      arbSetup.arbitrary,
      Gen.oneOf(TaskState.TASK_STARTING, TaskState.TASK_STAGING)
    ) { (setup, taskState) =>
      val mesosUpdate = buildMesosUpdate(taskState)(setup)
      setup.actorTestKit.run(MesosJobActor.Update(mesosUpdate))
      setup.jobDao wasNever called
      setup.mesosGateway.makeCall(setup.mesosStreamId, *) wasCalled once
    }

    "acknowledge TASK_RUNNING mesos update and update the job" in forAll { implicit setup: Setup =>
      val mesosUpdate = buildMesosUpdate(TaskState.TASK_RUNNING)
      setup.actorTestKit.run(MesosJobActor.Update(mesosUpdate))
      setup.jobDao.update(setup.jobId, *) wasCalled once
      setup.mesosGateway.makeCall(setup.mesosStreamId, *) wasCalled once
    }

    "acknowledge TASK_FINISHED mesos update and update the job" in forAll { implicit setup: Setup =>
      val mesosUpdate = buildMesosUpdate(TaskState.TASK_FINISHED)
      setup.actorTestKit.run(MesosJobActor.Update(mesosUpdate))
      setup.jobDao.update(setup.jobId, *) wasCalled once
      setup.mesosGateway.makeCall(setup.mesosStreamId, *) wasCalled once
    }

    "acknowledge TASK_UNREACHABLE, TASK_LOST or TASK_UNKNOWN mesos update, but do nothing" in forAll(
      arbSetup.arbitrary,
      Gen.oneOf(TaskState.TASK_UNREACHABLE, TaskState.TASK_LOST, TaskState.TASK_UNKNOWN)
    ) { (setup, taskState) =>
      val mesosUpdate = buildMesosUpdate(taskState)(setup)
      setup.actorTestKit.run(MesosJobActor.Update(mesosUpdate))
      setup.jobDao wasNever called
      setup.mesosGateway.makeCall(setup.mesosStreamId, *) wasCalled once
    }

    "acknowledge TASK_ERROR, TASK_GONE_BY_OPERATOR, TASK_DROPPED, TASK_KILLED, TASK_GONE or TASK_FAILED mesos update and update the job" in forAll(
      arbSetup.arbitrary,
      Gen.oneOf(
        TaskState.TASK_ERROR,
        TaskState.TASK_GONE_BY_OPERATOR,
        TaskState.TASK_DROPPED,
        TaskState.TASK_KILLED,
        TaskState.TASK_GONE,
        TaskState.TASK_FAILED
      )
    ) { (setup, taskState) =>
      val mesosUpdate = buildMesosUpdate(taskState)(setup)
      setup.actorTestKit.run(MesosJobActor.Update(mesosUpdate))
      setup.jobDao.update(setup.jobId, *) wasCalled once
      setup.mesosGateway.makeCall(setup.mesosStreamId, *) wasCalled once
    }

    "fail to process TASK_KILLING mesos update, but update the job" in forAll { implicit setup: Setup =>
      val mesosUpdate = buildMesosUpdate(TaskState.TASK_KILLING)
      intercept[IllegalArgumentException] {
        setup.actorTestKit.run(MesosJobActor.Update(mesosUpdate))
      }
      setup.jobDao.update(setup.jobId, *) wasCalled once
      setup.mesosGateway.makeCall(*, *) wasNever called
    }

  }

  def buildMesosUpdate(taskState: TaskState)(implicit setup: Setup): Update =
    Update
      .newBuilder()
      .setStatus(
        TaskStatus
          .newBuilder()
          .setState(taskState)
          .setTaskId(setup.taskId)
          .setUuid(ByteString.copyFromUtf8(UUID.randomUUID().toString))
      )
      .build()

}
