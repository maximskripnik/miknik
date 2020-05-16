package com.newflayer.miknik.core.providers

import com.newflayer.miknik.core.ClusterResourceManager
import com.newflayer.miknik.domain.Node
import com.newflayer.miknik.domain.Resources
import com.newflayer.miknik.utils.DurationConverters

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import java.time.Instant
import java.util.UUID

import akka.actor.typed.Scheduler
import akka.actor.typed.scaladsl.adapter._
import akka.pattern.after
import cats.data.NonEmptyList
import cats.implicits._
import com.decodified.scalassh.HostConfig
import com.decodified.scalassh.HostKeyVerifiers
import com.decodified.scalassh.PublicKeyLogin
import com.decodified.scalassh.SSH
import com.myjeeva.digitalocean.common.ActionStatus.COMPLETED
import com.myjeeva.digitalocean.common.ActionStatus.ERRORED
import com.myjeeva.digitalocean.common.ActionStatus.IN_PROGRESS
import com.myjeeva.digitalocean.common.DropletStatus.NEW
import com.myjeeva.digitalocean.exception.DigitalOceanException
import com.myjeeva.digitalocean.impl.DigitalOceanClient
import com.myjeeva.digitalocean.pojo.Droplet
import com.myjeeva.digitalocean.pojo.Image
import com.myjeeva.digitalocean.pojo.Key
import com.myjeeva.digitalocean.pojo.Region
import com.myjeeva.digitalocean.pojo.Size
import com.typesafe.config.Config

class DigitalOceanResourceManager private (
  api: DigitalOceanClient,
  image: Image,
  region: Region,
  sshKey: Key,
  sshLogin: PublicKeyLogin,
  updatePollPeriod: FiniteDuration,
  sizes: List[Size]
)(implicit ec: ExecutionContext, scheduler: akka.actor.Scheduler)
  extends ClusterResourceManager {

  def allocate(nodeResources: NonEmptyList[Resources]): Future[NonEmptyList[Node]] =
    for {
      droplets <- Future.fromTry(nodeResources.map(buildDroplet(_)).sequence)
      createdNodes <- droplets.map(createDroplet(_)).sequence
    } yield createdNodes

  def executeCommand(node: Node, cmd: List[String]): Future[Unit] =
    for {
      res <- Future {
        SSH.apply(
          node.ip, { host =>
            Success(
              HostConfig(
                login = sshLogin,
                hostName = host,
                hostKeyVerifier = HostKeyVerifiers.DontVerify
              )
            )
          }
        ) { client => client.exec(cmd.mkString(" ")) }
      }
      cmdRes <- Future.fromTry(res)
      _ <- Future.fromTry {
        Try {
          if (!cmdRes.exitCode.contains(0)) {
            val errorMsg = cmdRes.exitErrorMessage.getOrElse(cmdRes.stdErrAsString())
            throw new RuntimeException(
              s"Failed to execute cmd: $cmd. Exit code: ${cmdRes.exitCode} Error details: '$errorMsg'"
            )
          } else {
            ()
          }
        }
      }
    } yield ()

  def deallocate(nodeIds: NonEmptyList[String]): Future[Unit] =
    nodeIds.map(deallocateNode(_)).sequence.map(_ => ())

  private def buildDroplet(resources: Resources): Try[Droplet] = Try {
    val size = sizes
      .find { size =>
        size.getVirutalCpuCount() >= resources.cpus &&
        size.getMemorySizeInMb() >= resources.mem &&
        size.getDiskSize() >= resources.disk / 1024 // FIXME also account for image min disk size requirement
      }
      .getOrElse(
        throw new IllegalArgumentException(
          s"Required resources are too large: $resources. Could not find a DigitalOcean size to satisfy them"
        )
      )
    val droplet = new Droplet()
    val name = s"miknik-${UUID.randomUUID()}" // TODO move name generator to a separate place (to share between different resource managers impls)
    droplet.setSize(size.getSlug())
    droplet.setName(name)
    droplet.setRegion(region)
    droplet.setImage(image)
    droplet.setKeys(List(sshKey).asJava)
    droplet.setEnablePrivateNetworking(true)

    droplet
  }

  private def createDroplet(droplet: Droplet): Future[Node] =
    for {
      registeredDroplet <- Future(api.createDroplet(droplet))
      createdDroplet <- pollUntilCreated(registeredDroplet)
      dropletId = createdDroplet.getId()
      powerOnAction <- Future(api.powerOnDroplet(dropletId))
      _ <- pollUntilPoweredUp(powerOnAction.getId())
      (updatedDroplet, ip) <- pollUntilIpAllocated(createdDroplet)
      now = Instant.now()
      node = Node(
        id = updatedDroplet.getId().toString(),
        ip = ip,
        resources = Resources(
          mem = updatedDroplet.getMemorySizeInMb().toLong,
          cpus = updatedDroplet.getVirutalCpuCount().toDouble,
          disk = updatedDroplet.getDiskSize().toLong * 1024
        ),
        createdAt = now,
        lastUsedAt = now
      )
      _ <- pollUntilSSHIsReady(node) // it's not always available even after the droplet is up
    } yield node

  private def pollUntilCreated(droplet: Droplet): Future[Droplet] =
    if (droplet.getStatus() == NEW) {
      after(updatePollPeriod, scheduler) {
        Future(api.getDropletInfo(droplet.getId())).flatMap(pollUntilCreated(_))
      }
    } else {
      droplet.pure[Future]
    }

  private def pollUntilPoweredUp(powerOnActionId: Int): Future[Unit] =
    Future(api.getActionInfo(powerOnActionId)).flatMap { action =>
      action.getStatus() match {
        case COMPLETED =>
          ().pure[Future]
        case IN_PROGRESS =>
          after(updatePollPeriod, scheduler) {
            pollUntilPoweredUp(powerOnActionId)
          }
        case ERRORED =>
          (new RuntimeException(s"Failed to power up droplet '${action.getResourceId()}'")).raiseError[Future, Unit]
      }
    }

  private def pollUntilIpAllocated(droplet: Droplet): Future[(Droplet, String)] =
    droplet.getNetworks().getVersion4Networks().asScala.find(_.getType() == "private") match {
      case Some(network) =>
        (droplet, network.getIpAddress()).pure[Future]
      case None =>
        after(updatePollPeriod, scheduler) {
          Future(api.getDropletInfo(droplet.getId())).flatMap(pollUntilIpAllocated(_))
        }
    }

  private def pollUntilSSHIsReady(node: Node): Future[Unit] =
    executeCommand(node, List("test true")).transformWith {
      case Failure(_) => after(updatePollPeriod, scheduler)(pollUntilSSHIsReady(node))
      case Success(_) => ().pure[Future]
    }

  private def deallocateNode(nodeId: String): Future[Unit] =
    for {
      dropletId <- Future.fromTry(Try(nodeId.toInt))
      _ <- Future(api.deleteDroplet(dropletId))
      _ <- pollUntilDeleted(dropletId)
    } yield ()

  private def pollUntilDeleted(dropletId: Int): Future[Unit] =
    Future(api.getDropletInfo(dropletId))
      .transformWith {
        case Success(_) =>
          after(updatePollPeriod, scheduler) {
            pollUntilDeleted(dropletId)
          }
        case Failure(_: DigitalOceanException) =>
          ().pure[Future]
        case Failure(another) =>
          another.raiseError[Future, Unit]
      }
}

object DigitalOceanResourceManager {

  private case class Settings(
    authToken: String,
    image: Image,
    region: Region,
    sshKey: Key,
    sshLogin: PublicKeyLogin,
    updatePollPeriod: FiniteDuration
  )

  def apply(
    config: Config
  )(implicit ec: ExecutionContext, scheduler: Scheduler): Future[DigitalOceanResourceManager] =
    for {
      settings <- Future.fromTry(buildSettings(config))
      api = new DigitalOceanClient(settings.authToken)
      sizes <- Future(api.getAvailableSizes(0).getSizes().asScala.toList.sortBy(_.getPriceHourly))
    } yield new DigitalOceanResourceManager(
      api,
      settings.image,
      settings.region,
      settings.sshKey,
      settings.sshLogin,
      settings.updatePollPeriod,
      sizes
    )(ec, scheduler.toClassic)

  private def buildSettings(config: Config): Try[Settings] = Try {
    val authToken = config.getString("auth-token")
    val image = new Image(config.getInt("image-id"))
    val region = new Region(config.getString("region"))
    val updatePollPeriod = DurationConverters.toScalaDuration(config.getDuration("poll-period"))
    val sshKey = new Key(config.getInt("ssh-key-id"))
    val sshPrivateKeyPath =
      if (config.hasPath("ssh-private-key-path")) Some(config.getString("ssh-private-key-path")) else None
    val sshLogin = PublicKeyLogin(
      user = "root",
      sshPrivateKeyPath.fold(PublicKeyLogin.DefaultKeyLocations)(List(_)): _*
    )
    Settings(
      authToken,
      image,
      region,
      sshKey,
      sshLogin,
      updatePollPeriod
    )
  }

}
