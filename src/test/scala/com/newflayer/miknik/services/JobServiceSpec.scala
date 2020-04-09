package com.newflayer.miknik.services

import com.newflayer.miknik.BaseSpec
import com.newflayer.miknik.domain.Job
import com.newflayer.miknik.domain.JobGenerators
import com.newflayer.miknik.domain.JobStatus
import com.newflayer.miknik.domain.Resources
import com.newflayer.miknik.domain.ResourcesGenerators

import cats.data.NonEmptyList
import org.scalacheck.Arbitrary._

class JobServiceSpec extends BaseSpec with JobGenerators with ResourcesGenerators {

  abstract class Setup(data: List[Job] = List.empty) {
    val service = new JobService(data.distinctBy(_.id).map(job => job.id -> job).toMap)
  }

  "JobService#create" should {
    "create a job and return it" in new Setup {
      forAll {
        (
          id: String,
          resources: Resources,
          dockerImage: String,
          cmd: Option[List[String]],
          env: Option[Map[String, String]]
        ) =>
          whenReady(service.create(id, resources, dockerImage, cmd, env)) { job =>
            job.id shouldBe id
            job.resources shouldBe resources
            job.dockerImage shouldBe dockerImage
            job.cmd shouldBe cmd.getOrElse(List.empty)
            job.env shouldBe env.getOrElse(Map.empty)
          }
      }
    }
  }

  "JobService#list" should {
    "return existing jobs" in forAll { jobs: List[Job] =>
      new Setup(jobs) {
        whenReady(service.list()) { listResult =>
          listResult.count shouldBe jobs.size
          listResult.items should contain allElementsOf jobs
        }
      }
    }
  }

  "JobService#update" should {

    "set status to canceled and update the 'updated' field" in forAll { jobs: NonEmptyList[Job] =>
      new Setup(jobs.toList) {
        val job = jobs.head
        whenReady(service.update(job.id, status = Some(JobStatus.Canceled))) { result =>
          val updatedJob = result.right.value
          updatedJob.status shouldBe JobStatus.Canceled
          updatedJob.updated should not be job.updated
        }
      }
    }

    "do not update anything" in forAll { job: Job =>
      new Setup(List(job)) {
        whenReady(service.update(job.id, status = None))(_.right.value shouldBe job)
      }
    }

    "return error when job is not found" in forAll { (id: String, jobs: List[Job], status: Option[JobStatus]) =>
      whenever(!jobs.exists(_.id === id)) {
        new Setup(jobs) {
          whenReady(service.update(id, status))(_.left.value shouldBe JobService.UpdateError.NotFound(id))
        }
      }
    }

    "return error when given bad status" in {
      val statusGen = arbJobStatus.arbitrary.filterNot(_ === JobStatus.Canceled)
      forAll(arbJob.arbitrary, statusGen) { (job: Job, status: JobStatus) =>
        new Setup(List(job)) {
          whenReady(service.update(job.id, status = Some(status)))(
            _.left.value shouldBe JobService.UpdateError.BadStatus(status)
          )
        }
      }
    }

  }

  "JobService#delete" should {

    "delete a job" in forAll { (jobs: NonEmptyList[Job]) =>
      new Setup(jobs.toList) {
        val id = jobs.head.id
        whenReady(service.delete(id))(_.right.value shouldBe ())
      }
    }

    "return error when job is not found" in forAll { (id: String, jobs: List[Job]) =>
      whenever(!jobs.exists(_.id === id)) {
        new Setup(jobs) {
          whenReady(service.delete(id))(_.left.value shouldBe JobService.DeleteError.NotFound(id))
        }
      }
    }

  }

}
