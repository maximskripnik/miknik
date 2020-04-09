package com.newflayer.miknik.services

import com.newflayer.miknik.BaseSpec
import com.newflayer.miknik.core.WorkloadSupervisorActor
import com.newflayer.miknik.dao.JobDao
import com.newflayer.miknik.domain.Job
import com.newflayer.miknik.domain.JobGenerators
import com.newflayer.miknik.domain.JobStatus
import com.newflayer.miknik.domain.Resources
import com.newflayer.miknik.domain.ResourcesGenerators

import java.time.Instant

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import cats.implicits._
import org.scalacheck.Arbitrary._

class JobServiceSpec extends ScalaTestWithActorTestKit with BaseSpec with JobGenerators with ResourcesGenerators {

  trait Setup {
    val dao = mock[JobDao]
    val workloadSupervisorProbe = createTestProbe[WorkloadSupervisorActor.Message]()
    val workloadSupervisorActor = workloadSupervisorProbe.ref
    val service = new JobService(dao, workloadSupervisorActor)
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
          dao.create(*) returnsF job
          val result = service.create(id, resources, dockerImage, cmd, env)
          workloadSupervisorProbe.expectMessageType[WorkloadSupervisorActor.ScheduleJob]
          whenReady(result)(_ shouldBe job)
      }
    }
  }

  "JobService#list" should {
    "return existing jobs" in new Setup {
      forAll { jobs: List[Job] =>
        dao.list() returnsF jobs
        dao.count() returnsF jobs.size
        whenReady(service.list()) { listResult =>
          listResult.count shouldBe jobs.size
          listResult.items should contain allElementsOf jobs
        }
      }
    }
  }

  "JobService#update" should {

    "update the job and return the updated job when status is updated to canceled" in new Setup {
      forAll { job: Job =>
        dao.update(job.id, *) returnsF Some(
          job.copy(
            updated = Instant.now().plusMillis(1),
            status = JobStatus.Canceled
          )
        )
        whenReady(service.update(job.id, status = Some(JobStatus.Canceled))) { result =>
          val updatedJob = result.right.value
          updatedJob.status shouldBe JobStatus.Canceled
          updatedJob.updated should not be job.updated
        }
      }
    }

    "do not update anything, butn return a job" in new Setup {
      forAll { job: Job =>
        dao.get(job.id) returnsF Some(job)
        whenReady(service.update(job.id, status = None))(_.right.value shouldBe job)
      }
    }

    "return error when job is not found" in new Setup {

      forAll { id: String =>
        dao.update(id, *) returnsF None
        whenReady(service.update(id, status = Some(JobStatus.Canceled)))(
          _.left.value shouldBe JobService.UpdateError.NotFound(id)
        )
      }
    }

    "return error when given bad status" in {
      new Setup {
        forAll { (id: String, status: JobStatus) =>
          whenever(status != JobStatus.Canceled) {
            whenReady(service.update(id, status = Some(status)))(
              _.left.value shouldBe JobService.UpdateError.BadStatus(status)
            )
          }
        }
      }
    }

  }

  "JobService#delete" should {

    "delete a job" in new Setup {
      forAll { id: String =>
        dao.delete(id) returnsF true
        whenReady(service.delete(id))(_.right.value shouldBe ())
      }
    }

    "return error when job is not found" in new Setup {
      forAll { id: String =>
        dao.delete(id) returnsF false
        whenReady(service.delete(id))(_.left.value shouldBe JobService.DeleteError.NotFound(id))
      }
    }

  }

}
