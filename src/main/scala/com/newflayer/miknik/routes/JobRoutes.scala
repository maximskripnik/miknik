package com.newflayer.miknik.routes

import com.newflayer.miknik.domain.Job
import com.newflayer.miknik.routes.contracts.JobCreateRequest
import com.newflayer.miknik.routes.contracts.JobResponse
import com.newflayer.miknik.routes.contracts.JobUpdateRequest
import com.newflayer.miknik.routes.contracts.ListResponse
import com.newflayer.miknik.services.JobService
import com.newflayer.miknik.services.JobService.DeleteError
import com.newflayer.miknik.services.JobService.UpdateError

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._

class JobRoutes(service: JobService) extends Routes {

  def routes = pathPrefix("jobs") {
    pathEnd {
      (post & entity(as[JobCreateRequest])) { request =>
        onSuccess(
          service.create(
            id = request.id,
            resources = request.resources.toDomain,
            dockerImage = request.dockerImage,
            cmd = request.cmd,
            env = request.env
          )
        ) { job => complete(StatusCodes.Created, JobResponse(job)) }
      } ~
      get {
        onSuccess(service.list()) { listResult => complete(ListResponse(JobResponse(_: Job))(listResult)) }
      }
    } ~
    path(Segment) { id =>
      (patch & entity(as[JobUpdateRequest])) { request =>
        onSuccess(service.update(id, request.status)) {
          case Left(error) =>
            val code = error match {
              case UpdateError.NotFound(_) => StatusCodes.NotFound
              case UpdateError.BadStatus(_) => StatusCodes.BadRequest
            }
            complete(code)
          case Right(job) =>
            complete(JobResponse(job))
        }
      } ~
      delete {
        onSuccess(service.delete(id)) {
          case Left(error) =>
            val code = error match {
              case DeleteError.NotFound(_) => StatusCodes.NotFound
            }
            complete(code)
          case Right(_) =>
            complete()
        }
      }
    }
  }

}
