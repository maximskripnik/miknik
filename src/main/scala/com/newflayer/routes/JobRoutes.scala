package com.newflayer.routes

import com.newflayer.domain.Job
import com.newflayer.routes.contracts.JobCreateRequest
import com.newflayer.routes.contracts.JobResponse
import com.newflayer.routes.contracts.JobUpdateRequest
import com.newflayer.routes.contracts.ListResponse
import com.newflayer.services.JobService

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
          case Left(_) => complete(StatusCodes.BadRequest)
          case Right(job) => complete(JobResponse(job))
        }
      } ~
      delete {
        onSuccess(service.delete(id)) {
          case Left(_) => complete(StatusCodes.BadRequest)
          case Right(_) => complete()
        }
      }
    }
  }

}
