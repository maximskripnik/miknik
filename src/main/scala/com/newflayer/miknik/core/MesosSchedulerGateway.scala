package com.newflayer.miknik.core

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.ContentType
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaType
import akka.http.scaladsl.model.MediaType.Compressible
import akka.http.scaladsl.model.headers.Accept
import akka.http.scaladsl.model.headers.RawHeader
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.Offer
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.scheduler.Protos.Call.Decline
import org.slf4j.Logger

class MesosSchedulerGateway(masterAddress: String, http: HttpExt) {

  val apiUrl = s"http://$masterAddress/api/v1/scheduler"

  def makeCall(mesosStreamId: String, call: Call.Builder): Future[HttpResponse] =
    makeCall(mesosStreamId = Some(mesosStreamId), call)

  def makeAnonymousCall(call: Call.Builder): Future[HttpResponse] =
    makeCall(mesosStreamId = None, call)

  def declineOffers(
    mesosStreamId: String,
    frameworkId: FrameworkID,
    offers: List[Offer],
    log: Logger
  ): Future[HttpResponse] = {
    log.debug(s"Declining offers '${offers.map(_.getId)}'")
    makeCall(
      mesosStreamId,
      Call
        .newBuilder()
        .setType(Call.Type.DECLINE)
        .setFrameworkId(frameworkId)
        .setDecline(
          Decline
            .newBuilder()
            .addAllOfferIds(
              offers.map(_.getId).asJava
            )
        )
    )
  }

  private def makeCall(mesosStreamId: Option[String], call: Call.Builder): Future[HttpResponse] = {
    val baseHeaders = List[HttpHeader](Accept(MesosSchedulerGateway.protobufType))
    http.singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = apiUrl,
        headers = mesosStreamId.fold(
          baseHeaders
        )(streamId => RawHeader("Mesos-Stream-Id", streamId) :: baseHeaders),
        entity = HttpEntity(
          contentType = ContentType(MesosSchedulerGateway.protobufType),
          bytes = call.build.toByteArray
        )
      )
    )
  }

}

object MesosSchedulerGateway {

  val protobufType = MediaType.applicationBinary("x-protobuf", comp = Compressible)

}
