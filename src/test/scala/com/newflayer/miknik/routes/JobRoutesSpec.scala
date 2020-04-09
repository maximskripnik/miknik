package com.newflayer.miknik.routes

import com.newflayer.miknik.domain.Job
import com.newflayer.miknik.domain.JobGenerators
import com.newflayer.miknik.domain.JobStatus
import com.newflayer.miknik.domain.ListResult
import com.newflayer.miknik.routes.contracts.JobCreateRequest
import com.newflayer.miknik.routes.contracts.JobResponse
import com.newflayer.miknik.routes.contracts.JobResponse._
import com.newflayer.miknik.routes.contracts.JobUpdateRequest
import com.newflayer.miknik.routes.contracts.ListResponse
import com.newflayer.miknik.routes.contracts.Resources
import com.newflayer.miknik.services.JobService
import com.newflayer.miknik.services.JobService.DeleteError
import com.newflayer.miknik.services.JobService.UpdateError

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import cats.implicits._
import io.circe.Decoder
import io.circe.Encoder
import io.circe.Json
import io.circe.generic.semiauto._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

class JobRoutesSpec extends BaseRoutesSpec with JobGenerators {

  implicit val resourcesArbitrary: Arbitrary[Resources] = Arbitrary(arbResources.arbitrary.map(Resources(_)))

  implicit val jobCreateRequestArbitrary: Arbitrary[JobCreateRequest] = Arbitrary {
    for {
      id <- arbitrary[String]
      resources <- arbitrary[Resources]
      dockerImage <- arbitrary[String]
      cmd <- arbitrary[Option[List[String]]]
      env <- arbitrary[Option[Map[String, String]]]
    } yield JobCreateRequest(id, resources, dockerImage, cmd, env)
  }

  implicit val jobResponseDecoder: Decoder[JobResponse] = deriveDecoder[JobResponse]

  implicit val jobCreateRequestEncoder: Encoder[JobCreateRequest] = deriveEncoder[JobCreateRequest]

  implicit val jobUpdateRequestEncoder: Encoder[JobUpdateRequest] = deriveEncoder[JobUpdateRequest]

  trait Setup {
    val service = mock[JobService]
    val routes = Route.seal(new JobRoutes(service).routes)
  }

  "POST /jobs" should {

    "return 201 with a job" in new Setup {
      forAll { job: Job =>
        val jobRequest = JobCreateRequest(
          id = job.id,
          resources = Resources(job.resources),
          dockerImage = job.dockerImage,
          cmd = Some(job.cmd),
          env = Some(job.env)
        )
        service.create(job.id, job.resources, job.dockerImage, jobRequest.cmd, jobRequest.env) returnsF job
        Post("/jobs", jobRequest) ~> routes ~> check {
          status shouldBe StatusCodes.Created
          responseAs[Json].as[JobResponse] shouldBe Right(JobResponse(job))
        }
      }
    }

    "return 400 when body is invalid" in new Setup {
      Post("/jobs", Json.obj("foo" -> Json.fromString("bar"))) ~> routes ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

  }

  "GET /jobs" should {
    "return 200 with a list of jobs" in new Setup {
      forAll { jobs: List[Job] =>
        service.list returnsF ListResult(jobs)
        Get("/jobs") ~> routes ~> check {
          status shouldBe StatusCodes.OK
          responseAs[Json].as[ListResponse[JobResponse]] shouldBe Right(
            ListResponse(JobResponse(_: Job))(ListResult(jobs))
          )
        }
      }
    }
  }

  "PATCH /jobs/{id}" should {

    "return 200 with an updated job" in new Setup {
      forAll(nonEmptyAlphaNumString.arbitrary, arbitrary[Job]) { (id: String, _job: Job) =>
        val job = _job.copy(id = id)
        val updatedStatus = JobStatus.Canceled
        val updatedJob = job.copy(status = updatedStatus)
        service.update(job.id, Some(updatedStatus)) returnsF Right(updatedJob)
        Patch(s"/jobs/${job.id}", JobUpdateRequest(status = Some(updatedStatus))) ~> routes ~> check {
          status shouldBe StatusCodes.OK
          responseAs[Json].as[JobResponse] shouldBe Right(JobResponse(job.copy(status = updatedStatus)))
        }
      }
    }

    "return 400 when body is invalid" in new Setup {
      Patch("/jobs/id", Json.obj("status" -> Json.fromString("bar"))) ~> routes ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    "return 404 when service returns not found error" in new Setup {
      service.update("id", None) returnsF Left(UpdateError.NotFound("id"))
      Patch(s"/jobs/id", JobUpdateRequest(status = None)) ~> routes ~> check {
        status shouldBe StatusCodes.NotFound
      }
    }

    "return 400 when service returns bad status error" in new Setup {
      val updatedStatus = JobStatus.Completed
      service.update("id", Some(updatedStatus)) returnsF Left(UpdateError.BadStatus(updatedStatus))
      Patch(s"/jobs/id", JobUpdateRequest(status = Some(updatedStatus))) ~> routes ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

  }

  "DELETE /jobs/{id}" should {

    "return 200" in new Setup {
      forAll(nonEmptyAlphaNumString.arbitrary) { id =>
        service.delete(id) returnsF Right(())
        Delete(s"/jobs/$id") ~> routes ~> check {
          status shouldBe StatusCodes.OK
        }
      }
    }

    "return 404 when service returns not found error" in new Setup {
      service.delete("id") returnsF Left(DeleteError.NotFound("id"))
      Delete(s"/jobs/id") ~> routes ~> check {
        status shouldBe StatusCodes.NotFound
      }
    }

  }

}
