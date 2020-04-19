package com.newflayer.miknik.core

import com.newflayer.miknik.BaseSpec
import com.newflayer.miknik.dao.JobDao
import com.newflayer.miknik.domain.Job
import com.newflayer.miknik.domain.JobGenerators
import com.newflayer.miknik.domain.Resources

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.http.scaladsl.model.HttpResponse
import cats.data.NonEmptyList
import cats.implicits._
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.Offer
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskState
import org.apache.mesos.v1.scheduler.Protos.Event.Offers
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen

class WorkloadSupervisorActorSpec
  extends ScalaTestWithActorTestKit
  with BaseSpec
  with LogCapturing
  with JobGenerators
  with MesosProtoGenerators {

  class Setup(
    val mesosStreamId: String,
    val frameworkId: FrameworkID
  ) {
    val mesosGateway = mock[MesosSchedulerGateway]
    val jobDao = mock[JobDao]
    val actor = spawn(
      WorkloadSupervisorActor(
        mesosStreamId = mesosStreamId,
        frameworkId = frameworkId,
        mesosFrameworkActor = createTestProbe().ref,
        mesosGateway = mesosGateway,
        jobDao = jobDao
      )
    )
  }

  implicit val arbSetup: Arbitrary[Setup] = Arbitrary {
    for {
      mesosStreamId <- Gen.alphaNumStr
      frameworkId <- arbitrary[FrameworkID]
    } yield new Setup(mesosStreamId, frameworkId)
  }

  "WorkloadSupervisorActor" should {

    "schedule jobs in a queue" in forAll { (setup: Setup, jobs: List[Job]) =>
      jobs.foreach(setup.actor ! WorkloadSupervisorActor.ScheduleJob(_))
      val probe = createTestProbe[List[Job]]()
      setup.actor ! WorkloadSupervisorActor.GetQueue(probe.ref)
      probe.expectMessage(jobs)
    }

    "decline offers if the queue is empty" in forAll { (setup: Setup, offers: Offers) =>
      setup.mesosGateway.declineOffers(setup.mesosStreamId, setup.frameworkId, *, *) returnsF HttpResponse()

      setup.actor ! WorkloadSupervisorActor.Offers(offers)
      setup.mesosGateway.declineOffers(setup.mesosStreamId, setup.frameworkId, *, *) wasCalled (once within 1.second)
    }

    "decline offers if none satisfy queue head job resource requirements" in {
      forAll { (setup: Setup, queue: List[Job], oldestJob: Job, offers: List[Offer]) =>
        val mesosResources = List(
          buildScalarResource("cpus", 1.0),
          buildScalarResource("mem", 2048),
          buildScalarResource("disk", 4096)
        )
        val jobRequirements = Resources(
          cpus = 2.0,
          mem = 2048,
          disk = 4096
        )
        val mesosOffers = Offers.newBuilder.clearOffers
          .addAllOffers(
            offers.map(_.toBuilder.clearResources.addAllResources(mesosResources.asJava).build).asJava
          )
          .build

        setup.actor ! WorkloadSupervisorActor.ScheduleJob(oldestJob.copy(resources = jobRequirements))
        queue.foreach(setup.actor ! WorkloadSupervisorActor.ScheduleJob(_))
        setup.mesosGateway.declineOffers(setup.mesosStreamId, setup.frameworkId, *, *) returnsF HttpResponse()

        setup.actor ! WorkloadSupervisorActor.Offers(mesosOffers)
        setup.mesosGateway.declineOffers(setup.mesosStreamId, setup.frameworkId, *, *) wasCalled (once within 1.second)
      }
    }

    "accept an offer if it satisfies queue head job resource requirements, spawn a child actor and forward mesos updates to it" in {
      forAll { (setup: Setup, queue: List[Job], oldestJob: Job, offers: NonEmptyList[Offer], taskId: TaskID) =>
        val mesosResources = List(
          buildScalarResource("cpus", 2.0),
          buildScalarResource("mem", 4096),
          buildScalarResource("disk", 8192)
        )
        val jobRequirements = Resources(
          cpus = 2.0,
          mem = 2048,
          disk = 4096
        )
        val mesosOffers = Offers.newBuilder.clearOffers
          .addAllOffers(
            offers.map(_.toBuilder.clearResources.addAllResources(mesosResources.asJava).build).toList.asJava
          )
          .build

        setup.actor ! WorkloadSupervisorActor.ScheduleJob(oldestJob.copy(resources = jobRequirements))
        queue.foreach(setup.actor ! WorkloadSupervisorActor.ScheduleJob(_))
        setup.mesosGateway.makeCall(setup.mesosStreamId, *) returnsF HttpResponse()
        setup.mesosGateway.declineOffers(setup.mesosStreamId, setup.frameworkId, *, *) returnsF HttpResponse()

        setup.actor ! WorkloadSupervisorActor.Offers(mesosOffers)
        setup.mesosGateway.makeCall(setup.mesosStreamId, *) wasCalled (once within 1.second)

        val probe = createTestProbe[List[Job]]()
        setup.actor ! WorkloadSupervisorActor.GetQueue(probe.ref)
        probe.expectMessage(queue)

        val mesosUpdate = buildUpdate(taskId, TaskState.TASK_FINISHED)
        setup.actor ! WorkloadSupervisorActor.Update(mesosUpdate)
      }
    }

    "log an error and keep head job in the queue if an offer comes that satisfies that job resource requirements, but accept call fails" in {
      forAll { (setup: Setup, queue: List[Job], oldestJob: Job, offers: NonEmptyList[Offer]) =>
        val mesosResources = List(
          buildScalarResource("cpus", 2.0),
          buildScalarResource("mem", 4096),
          buildScalarResource("disk", 8192)
        )
        val jobRequirements = Resources(
          cpus = 2.0,
          mem = 2048,
          disk = 4096
        )
        val headQueueJob = oldestJob.copy(resources = jobRequirements)
        val mesosOffers = Offers.newBuilder.clearOffers
          .addAllOffers(
            offers.map(_.toBuilder.clearResources.addAllResources(mesosResources.asJava).build).toList.asJava
          )
          .build

        LoggingTestKit.error("Failed to accept offer").expect {
          setup.actor ! WorkloadSupervisorActor.ScheduleJob(headQueueJob)
          queue.foreach(setup.actor ! WorkloadSupervisorActor.ScheduleJob(_))
          setup.mesosGateway.makeCall(setup.mesosStreamId, *) returns Future.failed(new RuntimeException("boom"))
          setup.mesosGateway.declineOffers(setup.mesosStreamId, setup.frameworkId, *, *) returnsF HttpResponse()

          setup.actor ! WorkloadSupervisorActor.Offers(mesosOffers)
          setup.mesosGateway.makeCall(setup.mesosStreamId, *) wasCalled (once within 1.second)
        }

        val probe = createTestProbe[List[Job]]()
        setup.actor ! WorkloadSupervisorActor.GetQueue(probe.ref)
        probe.expectMessage(headQueueJob :: queue)
      }
    }

    case class CancelTestParams(setup: Setup, queue: NonEmptyList[Job], toCancelIndex: Int)

    implicit val arbParams: Arbitrary[CancelTestParams] = Arbitrary {
      for {
        setup <- arbitrary[Setup]
        queue <- arbitrary[NonEmptyList[Job]]
        toCancelIndex <- Gen.choose(0, queue.length - 1)
      } yield CancelTestParams(setup, queue, toCancelIndex)
    }

    "cancel a queued job" in {
      forAll(arbParams.arbitrary) {
        case CancelTestParams(setup, queue, toCancelIndex) =>
          queue.toList.foreach(setup.actor ! WorkloadSupervisorActor.ScheduleJob(_))

          val toCancel = queue.toList(toCancelIndex)
          val probe1 = createTestProbe[Boolean]()
          setup.actor ! WorkloadSupervisorActor.CancelJob(toCancel.id, probe1.ref)
          probe1.expectMessage(true)

          val expectedJobs = queue.toList.filter(_.id != toCancel.id)
          val probe2 = createTestProbe[List[Job]]()
          setup.actor ! WorkloadSupervisorActor.GetQueue(probe2.ref)
          probe2.expectMessage(expectedJobs)
      }
    }

    "cancel a running job" in {
      forAll { (setup: Setup, queue: List[Job], oldestJob: Job, offer: Offer) =>
        val mesosResources = List(
          buildScalarResource("cpus", oldestJob.resources.cpus),
          buildScalarResource("mem", oldestJob.resources.mem),
          buildScalarResource("disk", oldestJob.resources.disk)
        )
        val mesosOffers = Offers.newBuilder.clearOffers
          .addOffers(offer.toBuilder.clearResources.addAllResources(mesosResources.asJava))
          .build

        setup.actor ! WorkloadSupervisorActor.ScheduleJob(oldestJob)
        queue.toList.foreach(setup.actor ! WorkloadSupervisorActor.ScheduleJob(_))
        setup.mesosGateway.makeCall(setup.mesosStreamId, *) returnsF HttpResponse()

        setup.actor ! WorkloadSupervisorActor.Offers(mesosOffers)
        setup.mesosGateway.makeCall(setup.mesosStreamId, *) wasCalled (once within 1.second)

        val probe = createTestProbe[Boolean]()
        setup.actor ! WorkloadSupervisorActor.CancelJob(oldestJob.id, probe.ref)
        probe.expectMessage(true)
      }
    }

  }

}
