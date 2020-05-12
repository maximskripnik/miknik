package com.newflayer.miknik.core

import com.newflayer.miknik.core.MesosClusterManager.AgentLaunchResult
import com.newflayer.miknik.dao.JobDao
import com.newflayer.miknik.domain.BusyNode
import com.newflayer.miknik.domain.Job
import com.newflayer.miknik.domain.JobStatus
import com.newflayer.miknik.domain.Node
import com.newflayer.miknik.domain.Resources

import scala.collection.immutable.Nil
import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.model.HttpResponse
import cats.data.NonEmptyList
import org.apache.mesos.v1.Protos.AgentID
import org.apache.mesos.v1.Protos.CommandInfo
import org.apache.mesos.v1.Protos.ContainerInfo
import org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo
import org.apache.mesos.v1.Protos.Environment
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.Offer
import org.apache.mesos.v1.Protos.Offer.Operation
import org.apache.mesos.v1.Protos.OfferID
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskInfo
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.scheduler.Protos.Call.Accept
import org.apache.mesos.v1.scheduler.Protos.Event.{ Offers => MesosOffers, Update => MesosUpdate }
import org.slf4j.Logger

private class WorkloadSupervisorActor(
  mesosStreamId: String,
  frameworkId: FrameworkID,
  mesosGateway: MesosSchedulerGateway,
  jobDao: JobDao,
  clusterScaleDecisionMaker: ClusterScaleDecisionMaker,
  mesosClusterManager: MesosClusterManager,
  context: ActorContext[WorkloadSupervisorActor.Message]
)(implicit ec: ExecutionContext) {

  import WorkloadSupervisorActor._

  private def readyForOffers(state: State): Behavior[Message] =
    Behaviors
      .receiveMessagePartial[Message] {
        case ScheduleJob(job) =>
          scheduleJob(job, state, readyForOffers(_))
        case GetQueue(replyTo) =>
          getQueue(replyTo, state)
        case CancelJob(jobId, replyTo) =>
          cancelJob(jobId, replyTo, state, readyForOffers(_))
        case Offers(mesosOffers) =>
          handleOffers(mesosOffers, state, readyForOffers(_))
        case Update(mesosUpdate) =>
          handleUpdate(mesosUpdate, state)
        case OfferAccepted(offerId, job, taskId, agentId, nodeId) =>
          handleOfferAccepted(offerId, job, taskId, agentId, nodeId, state, readyForOffers(_))
        case OfferAcceptFailed(offerId, jobId, error) =>
          handleOfferAcceptFailed(offerId, jobId, error)
        case WorkerIsDone(taskId) =>
          handleWorkerIsDone(taskId, state, readyForOffers(_))
        case ClusterScaleTick =>
          handleClusterScaleTick(state)
      }

  private def waitingForClusterChanges(
    allocationIsDone: Boolean,
    deallocationIsDone: Boolean
  )(state: State): Behavior[Message] =
    Behaviors
      .receiveMessage[Message] {
        case AllocationPerformed(allocationResults) =>
          val newNodes = allocationResults.toList.foldLeft(List.empty[Node]) {
            case (soFar, AgentLaunchResult.AgentLaunched(node)) =>
              node :: soFar
            case (soFar, AgentLaunchResult.AgentLaunchFailed(node, error)) =>
              context.log.error(s"Failed to launch agent on node $node. Error: $error")
              soFar
          }
          if (newNodes.nonEmpty) {
            context.log.debug(s"Allocated new nodes: $newNodes")
          }
          val newState = state.copy(unusedNodes = state.unusedNodes ++ newNodes.map(node => (node.id -> node)))
          if (deallocationIsDone) {
            readyForOffers(newState)
          } else {
            waitingForClusterChanges(allocationIsDone = true, deallocationIsDone)(newState)
          }
        case AllocationFailed(error, resources) =>
          context.log.error(s"Failed to allocate the following resources: $resources. Error: '$error'")
          if (deallocationIsDone) {
            readyForOffers(state)
          } else {
            waitingForClusterChanges(allocationIsDone = true, deallocationIsDone)(state)
          }
        case DeallocationPerformed(deallocatedIds) =>
          context.log.debug(s"Deallocated the following nodes: $deallocatedIds")
          val newState = state.copy(unusedNodes = state.unusedNodes.filterNot {
            case (id, _) => deallocatedIds.exists(_ == id)
          })
          if (allocationIsDone) {
            readyForOffers(newState)
          } else {
            waitingForClusterChanges(allocationIsDone, deallocationIsDone = true)(newState)
          }
        case DeallocationFailed(error, nodeIds) =>
          context.log.error(s"Failed to deallocate the following nodes: $nodeIds. Error: '$error'")
          if (allocationIsDone) {
            readyForOffers(state)
          } else {
            waitingForClusterChanges(allocationIsDone, deallocationIsDone = true)(state)
          }
        case ScheduleJob(job) =>
          scheduleJob(job, state, waitingForClusterChanges(allocationIsDone, deallocationIsDone))
        case GetQueue(replyTo) =>
          getQueue(replyTo, state)
        case CancelJob(jobId, replyTo) =>
          cancelJob(jobId, replyTo, state, waitingForClusterChanges(allocationIsDone, deallocationIsDone))
        case Offers(mesosOffers) =>
          handleOffers(mesosOffers, state, waitingForClusterChanges(allocationIsDone, deallocationIsDone))
        case Update(mesosUpdate) =>
          handleUpdate(mesosUpdate, state)
        case OfferAccepted(offerId, job, taskId, agentId, nodeId) =>
          handleOfferAccepted(
            offerId,
            job,
            taskId,
            agentId,
            nodeId,
            state,
            waitingForClusterChanges(allocationIsDone, deallocationIsDone)
          )
        case OfferAcceptFailed(offerId, jobId, error) =>
          handleOfferAcceptFailed(offerId, jobId, error)
        case WorkerIsDone(taskId) =>
          handleWorkerIsDone(taskId, state, waitingForClusterChanges(allocationIsDone, deallocationIsDone))
        case ClusterScaleTick =>
          val msg = "Cluster change tick received before previous tick was processed." +
            "Either the tick duration is too small (try increasing it) or cluster resource manager takes too long to scale the cluster"
          context.log.warn(msg)
          Behaviors.same
      }

  private def handleOffers(
    mesosOffers: MesosOffers,
    state: State,
    transition: Transition
  ): Behavior[Message] = {
    val offers = mesosOffers.getOffersList.asScala.toList
    state.queue.headOption match {
      case Some(job) =>
        val acceptableOffer = offers.find((offerIsGoodEnough(_, job)))
        acceptableOffer match {
          case Some(offer) =>
            val taskId = TaskID.newBuilder.setValue(job.id).build
            context.pipeToSelf(acceptOffer(offer, job, taskId, context.log)) {
              case Success(_) =>
                val nodeId = offer.getAttributesList.asScala.find(_.getName == "node_id").map { attribute =>
                  attribute.getText.toString
                }
                OfferAccepted(offer.getId, job, taskId, offer.getAgentId, nodeId)
              case Failure(e) =>
                OfferAcceptFailed(offer.getId, job.id, e)
            }
            mesosGateway.declineOffers(
              mesosStreamId,
              frameworkId,
              offers.filterNot(_.getId == offer.getId),
              context.log
            )
            Behaviors.same
          case None =>
            mesosGateway.declineOffers(mesosStreamId, frameworkId, offers, context.log)
            Behaviors.same
        }
      case None =>
        mesosGateway.declineOffers(mesosStreamId, frameworkId, offers, context.log)
        transition(state)
    }
  }

  private def scheduleJob(job: Job, state: State, transition: Transition): Behavior[Message] =
    transition(state.copy(queue = state.queue.enqueue(job)))

  private def getQueue(replyTo: ActorRef[List[Job]], state: State): Behavior[Message] = {
    replyTo ! state.queue.toList
    Behaviors.same
  }

  private def cancelJob(
    jobId: String,
    replyTo: ActorRef[Boolean],
    state: State,
    transition: Transition
  ): Behavior[Message] =
    state.jobIdIndex.get(jobId) match {
      case Some(taskId) =>
        state.workers.get(taskId) match {
          case Some((worker, _)) =>
            worker ! MesosJobActor.Cancel(context.system.ignoreRef)
            replyTo ! true
            Behaviors.same
          case None =>
            context.log.error(
              s"Job index points to a task that has no worker. Job id: '$jobId'. Task id: '$taskId'"
            )
            replyTo ! false
            Behaviors.same
        }
      case None =>
        val (leftJobs, right) = state.queue.span(_.id != jobId)
        right.dequeueOption match {
          case Some((_, rightJobs)) =>
            context.log.debug(s"Job '$jobId' has been cancelled while in queue")
            jobDao.update(jobId, _.copy(status = JobStatus.Canceled))
            replyTo ! true
            transition(state.copy(queue = leftJobs.appendedAll(rightJobs)))
          case None =>
            replyTo ! false
            Behaviors.same
        }
    }

  private def handleUpdate(update: MesosUpdate, state: State): Behavior[Message] = {
    val taskId = update.getStatus.getTaskId
    state.workers.get(taskId) match {
      case Some((worker, _)) =>
        worker ! MesosJobActor.Update(update)
      case None =>
        context.log.warn(s"Received an update for an unknown task: '$taskId'")
    }
    Behaviors.same
  }

  private def handleOfferAccepted(
    offerId: OfferID,
    job: Job,
    taskId: TaskID,
    agentId: AgentID,
    nodeId: Option[NodeId],
    state: State,
    transition: Transition
  ): Behavior[Message] = {
    context.log.debug(s"Accepted offer '$offerId' for job '${job.id}'. Spawning child actor")
    val worker = context.spawnAnonymous(
      MesosJobActor(
        job.id,
        jobDao,
        mesosStreamId,
        frameworkId,
        agentId,
        taskId,
        mesosGateway
      )
    )
    context.watchWith(worker, WorkerIsDone(taskId))
    val newQueue = state.queue.filterNot(_.id == job.id)
    val (newBusyNodes, newUnusedNodes) = nodeId match {
      case Some(id) =>
        state.busyNodes.get(id) match {
          case Some(busyNode) =>
            (state.busyNodes.updated(id, busyNode.copy(runningJobs = job :: busyNode.runningJobs)), state.unusedNodes)
          case None =>
            state.unusedNodes.get(id) match {
              case Some(node) =>
                (state.busyNodes.updated(id, BusyNode(node, NonEmptyList.of(job))), state.unusedNodes.removed(id))
              case None =>
                context.log.error(s"Unknown node id '$id' in Mesos offer '$offerId'")
                (state.busyNodes, state.unusedNodes)
            }
        }
      case None =>
        (state.busyNodes, state.unusedNodes)
    }
    transition(
      state.copy(
        queue = newQueue,
        workers = state.workers.updated(taskId, worker -> job.id),
        jobIdIndex = state.jobIdIndex.updated(job.id, taskId),
        busyNodes = newBusyNodes,
        unusedNodes = newUnusedNodes
      )
    )
  }

  private def handleOfferAcceptFailed(offerId: OfferID, jobId: String, error: Throwable): Behavior[Message] = {
    context.log.error(
      s"Failed to accept offer '$offerId' for job '$jobId'",
      error
    )
    Behaviors.same
  }

  private def handleWorkerIsDone(taskId: TaskID, state: State, transition: Transition): Behavior[Message] =
    state.workers.get(taskId) match {
      case Some((_, jobId)) =>
        transition(
          state.copy(
            workers = state.workers.removed(taskId),
            jobIdIndex = state.jobIdIndex.removed(jobId)
          )
        )
      case None =>
        context.log.error(s"Unknown worker with task id '$taskId' reported that it is done")
        Behaviors.same
    }

  private def handleClusterScaleTick(state: State): Behavior[Message] =
    clusterScaleDecisionMaker.decideClusterScale(
      state.queue.toList,
      state.busyNodes.values.toList,
      state.unusedNodes.values.toList
    ) match {
      case Some(clusterChanges) =>
        (clusterChanges.add, clusterChanges.remove) match {
          case (resources :: moreResources, Nil) =>
            registerForAllocationPerformed(NonEmptyList(resources, moreResources))
            waitingForClusterChanges(allocationIsDone = false, deallocationIsDone = true)(state)
          case (Nil, nodes @ _ :: _) =>
            if (registerFordeallocationPerformed(nodes, state.busyNodes)) {
              waitingForClusterChanges(allocationIsDone = true, deallocationIsDone = false)(state)
            } else {
              Behaviors.same
            }
          case (resources :: moreResources, nodes @ _ :: _) =>
            registerForAllocationPerformed(NonEmptyList(resources, moreResources))
            if (registerFordeallocationPerformed(nodes, state.busyNodes)) {
              waitingForClusterChanges(allocationIsDone = false, deallocationIsDone = false)(state)
            } else {
              waitingForClusterChanges(allocationIsDone = false, deallocationIsDone = true)(state)
            }
          case (Nil, Nil) =>
            Behaviors.same
        }
      case None =>
        Behaviors.same
    }

  private def registerForAllocationPerformed(resources: NonEmptyList[Resources]): Unit = {
    context.pipeToSelf(mesosClusterManager.allocate(resources)) {
      case Success(nodes) => AllocationPerformed(nodes)
      case Failure(error) => AllocationFailed(error, resources)
    }
  }

  private def registerFordeallocationPerformed(
    nodesToDeallocate: List[Node],
    currentBusyNodes: Map[NodeId, BusyNode]
  ): Boolean = {
    val (incorrectNodes, correctNodes) =
      nodesToDeallocate.partition(node => currentBusyNodes.keys.exists(_ == node.id))
    if (incorrectNodes.nonEmpty) {
      context.log.warn(
        s"Decision was made to deallocate the following nodes that are being used: ${incorrectNodes.mkString("\n")}\nThey shall NOT be deallocated"
      )
    }
    correctNodes match {
      case node :: nodes =>
        val nodeIds = NonEmptyList(node, nodes).map(_.id)
        context.pipeToSelf(mesosClusterManager.deallocate(nodeIds)) {
          case Success(_) => DeallocationPerformed(nodeIds)
          case Failure(error) => DeallocationFailed(error, nodeIds)
        }
        true
      case Nil =>
        false
    }
  }

  private def acceptOffer(
    offer: Offer,
    job: Job,
    taskId: TaskID,
    log: Logger
  ): Future[HttpResponse] = {
    log.debug(s"Accepting offer '${offer.getId}'")

    val taskInfo = TaskInfo
      .newBuilder()
      .setName(job.id)
      .setTaskId(taskId)
      .setAgentId(offer.getAgentId())
      .addAllResources(offer.getResourcesList)
      .setCommand(
        CommandInfo
          .newBuilder()
          .setShell(false)
          .addAllArguments(job.cmd.asJava)
          .setEnvironment(
            Environment
              .newBuilder()
              .addAllVariables(
                job.env.toList.map {
                  case (name, value) =>
                    Environment.Variable
                      .newBuilder()
                      .setName(name)
                      .setValue(value)
                      .build
                }.asJava
              )
          )
      )
      .setContainer(
        ContainerInfo
          .newBuilder()
          .setType(ContainerInfo.Type.DOCKER)
          .setDocker(DockerInfo.newBuilder().setImage(job.dockerImage))
      )

    mesosGateway.makeCall(
      mesosStreamId,
      Call
        .newBuilder()
        .setType(Call.Type.ACCEPT)
        .setFrameworkId(frameworkId)
        .setAccept(
          Accept
            .newBuilder()
            .addOfferIds(offer.getId)
            .addOperations(
              Operation
                .newBuilder()
                .setType(Operation.Type.LAUNCH)
                .setLaunch(
                  Operation.Launch
                    .newBuilder()
                    .addTaskInfos(taskInfo)
                )
            )
        )
    )

  }

  private def offerIsGoodEnough(
    offer: Offer,
    job: Job
  ): Boolean = {
    val resources = offer.getResourcesList.asScala.toList
    resources.exists(resource => resource.getName == "cpus" && resource.getScalar.getValue >= job.resources.cpus) &&
    resources.exists(resource => resource.getName == "mem" && resource.getScalar.getValue >= job.resources.mem) &&
    resources.exists(resource => resource.getName == "disk" && resource.getScalar.getValue >= job.resources.disk)
  }

}

object WorkloadSupervisorActor {

  sealed trait Message
  case class ScheduleJob(job: Job) extends Message
  case class CancelJob(jobId: String, replyTo: ActorRef[Boolean]) extends Message
  case class Offers(mesosOffers: MesosOffers) extends Message
  case class Update(mesosUpdate: MesosUpdate) extends Message
  case class GetQueue(replyTo: ActorRef[List[Job]]) extends Message

  private case class OfferAccepted(
    offerId: OfferID,
    job: Job,
    taskId: TaskID,
    agentId: AgentID,
    nodeId: Option[NodeId]
  ) extends Message
  private case class OfferAcceptFailed(offerId: OfferID, jobId: String, error: Throwable) extends Message
  private case class WorkerIsDone(taskId: TaskID) extends Message
  private case object ClusterScaleTick extends Message
  private case class AllocationPerformed(allocationResults: NonEmptyList[AgentLaunchResult]) extends Message
  private case class AllocationFailed(error: Throwable, resources: NonEmptyList[Resources]) extends Message
  private case class DeallocationPerformed(deallocatedIds: NonEmptyList[String]) extends Message
  private case class DeallocationFailed(error: Throwable, nodeIds: NonEmptyList[String]) extends Message

  private type JobId = String
  private type NodeId = String
  private type Transition = State => Behavior[Message]

  private case class State(
    queue: Queue[Job],
    workers: Map[TaskID, (ActorRef[MesosJobActor.Message], JobId)],
    jobIdIndex: Map[JobId, TaskID],
    busyNodes: Map[NodeId, BusyNode],
    unusedNodes: Map[NodeId, Node]
  )

  private case class AllocatedNode(node: Node, runningJobs: List[Job])

  def apply(
    mesosStreamId: String,
    frameworkId: FrameworkID,
    mesosFrameworkActor: ActorRef[MesosFrameworkActor.Message],
    mesosGateway: MesosSchedulerGateway,
    jobDao: JobDao,
    clusterChangeDecisionMaker: ClusterScaleDecisionMaker,
    mesosClusterManager: MesosClusterManager,
    clusterScaleTickPeriod: FiniteDuration
  )(implicit ec: ExecutionContext): Behavior[Message] =
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        mesosFrameworkActor ! MesosFrameworkActor.SubscribeToMesosOffers(context.messageAdapter(Offers(_)))
        mesosFrameworkActor ! MesosFrameworkActor.SubscribeToMesosUpdates(context.messageAdapter(Update(_)))

        timers.startTimerWithFixedDelay(ClusterScaleTick, clusterScaleTickPeriod)

        new WorkloadSupervisorActor(
          mesosStreamId,
          frameworkId,
          mesosGateway,
          jobDao,
          clusterChangeDecisionMaker,
          mesosClusterManager,
          context
        ).readyForOffers(
          State(
            Queue.empty,
            Map.empty,
            Map.empty,
            Map.empty, // FIXME load from dao
            Map.empty // FIXME load from dao
          )
        )
      }
    }

}
