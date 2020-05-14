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
import com.google.protobuf.AbstractMessageLite
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.Offer
import org.apache.mesos.v1.master.Protos.{ Call => MasterCall }
import org.apache.mesos.v1.scheduler.Protos.Call.Decline
import org.apache.mesos.v1.scheduler.Protos.{ Call => SchedulerCall }
import org.slf4j.Logger

class MesosHttpGateway(masterAddress: String, http: HttpExt) {

  val schedulerApiUrl = s"http://$masterAddress/api/v1/scheduler"
  val masterApiUrl = s"http://$masterAddress/api/v1"

  def makeSchedulerCall(mesosStreamId: String, call: SchedulerCall.Builder): Future[HttpResponse] =
    makeSchedulerCall(mesosStreamId = Some(mesosStreamId), call)

  def makeAnonymousSchedulerCall(call: SchedulerCall.Builder): Future[HttpResponse] =
    makeSchedulerCall(mesosStreamId = None, call)

  def makeMasterCall(call: MasterCall.Builder): Future[HttpResponse] =
    makeHttpRequest(masterApiUrl, mesosStreamId = None, call.build())

  def declineOffers(
    mesosStreamId: String,
    frameworkId: FrameworkID,
    offers: List[Offer],
    log: Logger
  ): Future[HttpResponse] = {
    log.debug(s"Declining offers '${offers.map(_.getId)}'")
    makeSchedulerCall(
      mesosStreamId,
      SchedulerCall
        .newBuilder()
        .setType(SchedulerCall.Type.DECLINE)
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

  private def makeSchedulerCall(mesosStreamId: Option[String], call: SchedulerCall.Builder): Future[HttpResponse] =
    makeHttpRequest(schedulerApiUrl, mesosStreamId, call.build())

  private def makeHttpRequest(
    apiUrl: String,
    mesosStreamId: Option[String],
    protobuf: AbstractMessageLite[_, _]
  ): Future[HttpResponse] = {
    val baseHeaders = List[HttpHeader](Accept(MesosHttpGateway.protobufType))
    http.singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = apiUrl,
        headers = mesosStreamId.fold(
          baseHeaders
        )(streamId => RawHeader("Mesos-Stream-Id", streamId) :: baseHeaders),
        entity = HttpEntity(
          contentType = ContentType(MesosHttpGateway.protobufType),
          bytes = protobuf.toByteArray
        )
      )
    )
  }

}

object MesosHttpGateway {

  val protobufType = MediaType.applicationBinary("x-protobuf", comp = Compressible)

}
