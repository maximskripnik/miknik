package com.newflayer.miknik.services

import com.newflayer.miknik.BaseSpec
import com.newflayer.miknik.core.WorkloadSupervisorActor
import com.newflayer.miknik.dao.JobDao
import com.newflayer.miknik.domain.Job
import com.newflayer.miknik.domain.JobGenerators
import com.newflayer.miknik.domain.JobStatus
import com.newflayer.miknik.domain.Resources
import com.newflayer.miknik.domain.ResourcesGenerators

import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import cats.implicits._
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen

class JobServiceSpec extends ScalaTestWithActorTestKit with BaseSpec with JobGenerators with ResourcesGenerators {

  trait Setup {
    val dao = mock[JobDao]
    val workloadSupervisorProbe = createTestProbe[WorkloadSupervisorActor.Message]()
    val workloadSupervisorActor = workloadSupervisorProbe.ref
    val service = new JobService(dao, workloadSupervisorActor)(ec, system.scheduler, 100.millis)
  }

  "JobService#create" should {
    "create a job and return it" in new Setup {
      forAll {
        (
          id: String,
          resources: Resources,
          dockerImage: String,
          cmd: Option[List[String]],
          env: Option[Map[String, String]],
          job: Job
        ) =>
          dao.get(id) returnsF None
          dao.create(*) returnsF job
          val result = service.create(id, resources, dockerImage, cmd, env)
          workloadSupervisorProbe.expectMessageType[WorkloadSupervisorActor.ScheduleJob]
          whenReady(result)(_ shouldBe job)
      }
    }
  }

  "JobService#list" should {
    "return existing jobs" in forAll { jobs: List[Job] =>
      new Setup {
        dao.list() returnsF jobs
        dao.count() returnsF jobs.size
        whenReady(service.list()) { listResult =>
          listResult.count shouldBe jobs.size
          listResult.items should contain allElementsOf jobs
        }
      }
    }
  }

  "JobService#cancel" should {

    "cancel a non-terminated job" in forAll(
      arbitrary[Job],
      Gen.oneOf(JobStatus.Pending, JobStatus.Running)
    ) { (job, status) =>
      new Setup {
        dao.get(job.id) returnsF Some(job.copy(status = status))

        val result = service.cancel(job.id)
        val cancelMessage = workloadSupervisorProbe.expectMessageType[WorkloadSupervisorActor.CancelJob]
        cancelMessage.replyTo ! true

        whenReady(result)(_ shouldBe ().asRight)
      }
    }

    "return error when job is not found" in forAll { id: String =>
      new Setup {
        dao.get(id) returnsF None
        whenReady(service.cancel(id))(
          _.left.value shouldBe JobService.CancelError.NotFound(id)
        )
      }
    }

    "return error when job has terminated status" in forAll(
      arbitrary[Job],
      Gen.oneOf(JobStatus.Completed, JobStatus.Failed, JobStatus.Canceled)
    ) { (job, status) =>
      new Setup {
        dao.get(job.id) returnsF Some(job.copy(status = status))
        whenReady(service.cancel(job.id))(
          _.left.value shouldBe JobService.CancelError.BadStatus(status)
        )
      }
    }

  }

  "JobService#delete" should {

    "delete a job" in forAll { id: String =>
      new Setup {
        dao.delete(id) returnsF true
        whenReady(service.delete(id))(_.right.value shouldBe ())
      }
    }

    "return error when job is not found" in forAll { id: String =>
      new Setup {
        dao.delete(id) returnsF false
        whenReady(service.delete(id))(_.left.value shouldBe JobService.DeleteError.NotFound(id))
      }
    }

  }

}
