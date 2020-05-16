package com.newflayer.miknik.core

import com.newflayer.miknik.core.MesosClusterManager.DeallocationParams
import com.newflayer.miknik.core.MesosClusterManagerActor.AgentLaunchResult
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

import java.time.Instant

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
  mesosGateway: MesosHttpGateway,
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
          handleOffers(mesosOffers, state)
        case Update(mesosUpdate) =>
          handleUpdate(mesosUpdate, state)
        case OfferAccepted(offerId, job, taskId, agentId) =>
          handleOfferAccepted(offerId, job, taskId, agentId, state, readyForOffers(_))
        case OfferAcceptFailed(offerId, agentId, jobId, error) =>
          handleOfferAcceptFailed(offerId, agentId, jobId, error, state, readyForOffers(_))
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
          val newNodes = allocationResults.toList.foldLeft(List.empty[(Node, AgentID)]) {
            case (soFar, AgentLaunchResult.AgentLaunched(node, agentId)) =>
              node -> agentId :: soFar
            case (soFar, AgentLaunchResult.AgentLaunchFailed(node, error)) =>
              context.log.error(s"Failed to launch agent on node $node. Error: $error")
              soFar
          }
          if (newNodes.nonEmpty) {
            context.log.debug(s"Allocated new nodes: $newNodes")
          }
          val (newUnusedNodes, newNodeIdIndex) = newNodes.foldLeft((state.unusedNodes, state.nodeIdIndex)) {
            case ((unusedNodes, nodeIdIndex), (node, agentId)) =>
              (unusedNodes.updated(agentId, node), nodeIdIndex.updated(node.id, agentId))
          }
          val newState = state.copy(unusedNodes = newUnusedNodes, nodeIdIndex = newNodeIdIndex)
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
        case DeallocationPerformed(deallocationParams) =>
          context.log.debug(s"Deallocated the following nodes: $deallocationParams")
          val newUnusedNodes = state.unusedNodes.filterNot {
            case (id, _) => deallocationParams.exists(params => state.nodeIdIndex(params.nodeId) == id)
          }
          val newNodeIdIndex = state.nodeIdIndex.removedAll(deallocationParams.map(_.nodeId).toList)
          val newState = state.copy(unusedNodes = newUnusedNodes, nodeIdIndex = newNodeIdIndex)
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
          mesosGateway.declineOffers(mesosStreamId, frameworkId, mesosOffers.getOffersList.asScala.toList, context.log)
          Behaviors.same
        case Update(mesosUpdate) =>
          handleUpdate(mesosUpdate, state)
        case OfferAccepted(offerId, job, taskId, agentId) =>
          handleOfferAccepted(
            offerId,
            job,
            taskId,
            agentId,
            state,
            waitingForClusterChanges(allocationIsDone, deallocationIsDone)
          )
        case OfferAcceptFailed(offerId, agentId, jobId, error) =>
          handleOfferAcceptFailed(
            offerId,
            agentId,
            jobId,
            error,
            state,
            waitingForClusterChanges(allocationIsDone, deallocationIsDone)
          )
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
    state: State
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
                OfferAccepted(offer.getId, job, taskId, offer.getAgentId)
              case Failure(e) =>
                OfferAcceptFailed(offer.getId, offer.getAgentId, job.id, e)
            }
            val (newBusyNodes, newUnusedNodes) =
              addJobToNode(state.busyNodes, state.unusedNodes, offer.getAgentId(), job)
            val unnecessaryOffers = offers.filterNot(_.getId == offer.getId)
            if (unnecessaryOffers.nonEmpty) {
              mesosGateway.declineOffers(
                mesosStreamId,
                frameworkId,
                unnecessaryOffers,
                context.log
              )
            }
            readyForOffers(state.copy(busyNodes = newBusyNodes, unusedNodes = newUnusedNodes))
          case None =>
            mesosGateway.declineOffers(mesosStreamId, frameworkId, offers, context.log)
            Behaviors.same
        }
      case None =>
        mesosGateway.declineOffers(mesosStreamId, frameworkId, offers, context.log)
        readyForOffers(state)
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
          case Some(Worker(worker, _, _)) =>
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
      case Some(Worker(worker, _, _)) =>
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
    transition(
      state.copy(
        queue = newQueue,
        workers = state.workers.updated(taskId, Worker(worker, job.id, agentId)),
        jobIdIndex = state.jobIdIndex.updated(job.id, taskId)
      )
    )
  }

  private def handleOfferAcceptFailed(
    offerId: OfferID,
    agentId: AgentID,
    jobId: String,
    error: Throwable,
    state: State,
    transition: Transition
  ): Behavior[Message] = {
    context.log.error(
      s"Failed to accept offer '$offerId' for job '$jobId'",
      error
    )
    val (newBusyNodes, newUnusedNodes) = removeJobFromNode(state.busyNodes, state.unusedNodes, agentId, jobId)
    transition(state.copy(busyNodes = newBusyNodes, unusedNodes = newUnusedNodes))
  }

  private def handleWorkerIsDone(taskId: TaskID, state: State, transition: Transition): Behavior[Message] =
    state.workers.get(taskId) match {
      case Some(Worker(_, jobId, agentId)) =>
        val (newBusyNodes, newUnusedNodes) = removeJobFromNode(state.busyNodes, state.unusedNodes, agentId, jobId)
        transition(
          state.copy(
            workers = state.workers.removed(taskId),
            jobIdIndex = state.jobIdIndex.removed(jobId),
            busyNodes = newBusyNodes,
            unusedNodes = newUnusedNodes
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
            if (registerFordeallocationPerformed(nodes, state.nodeIdIndex, state.busyNodes)) {
              waitingForClusterChanges(allocationIsDone = true, deallocationIsDone = false)(state)
            } else {
              Behaviors.same
            }
          case (resources :: moreResources, nodes @ _ :: _) =>
            registerForAllocationPerformed(NonEmptyList(resources, moreResources))
            if (registerFordeallocationPerformed(nodes, state.nodeIdIndex, state.busyNodes)) {
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
    currentNodeIdIndex: Map[NodeId, AgentID],
    currentBusyNodes: Map[AgentID, BusyNode]
  ): Boolean = {
    val (incorrectNodes, correctNodes) = nodesToDeallocate.partition { node =>
      currentBusyNodes.contains(currentNodeIdIndex(node.id))
    }
    if (incorrectNodes.nonEmpty) {
      context.log.warn(
        s"Decision was made to deallocate the following nodes that are being used: ${incorrectNodes.mkString("\n")}. They shall NOT be deallocated"
      )
    }
    correctNodes match {
      case node :: nodes =>
        val deallocationParams = NonEmptyList(node, nodes).map { node =>
          DeallocationParams(node.id, currentNodeIdIndex(node.id))
        }
        context.pipeToSelf(mesosClusterManager.deallocate(deallocationParams)) {
          case Success(_) => DeallocationPerformed(deallocationParams)
          case Failure(error) => DeallocationFailed(error, deallocationParams)
        }
        true
      case Nil =>
        false
    }
  }

  private def addJobToNode(
    busyNodes: Map[AgentID, BusyNode],
    unusedNodes: Map[AgentID, Node],
    agentId: AgentID,
    job: Job
  ): (Map[AgentID, BusyNode], Map[AgentID, Node]) =
    busyNodes.get(agentId) match {
      case Some(busyNode) =>
        (
          busyNodes.updated(
            agentId,
            busyNode.copy(
              runningJobs = job :: busyNode.runningJobs,
              node = busyNode.node.copy(lastUsedAt = Instant.now())
            )
          ),
          unusedNodes
        )
      case None =>
        unusedNodes.get(agentId) match {
          case Some(node) =>
            (
              busyNodes.updated(agentId, BusyNode(node.copy(lastUsedAt = Instant.now()), NonEmptyList.of(job))),
              unusedNodes.removed(agentId)
            )
          case None =>
            (busyNodes, unusedNodes)
        }
    }

  private def removeJobFromNode(
    busyNodes: Map[AgentID, BusyNode],
    unusedNodes: Map[AgentID, Node],
    agentId: AgentID,
    jobId: JobId
  ) =
    busyNodes.get(agentId) match {
      case Some(thisBusyNode) =>
        val newNodeRunningJobs = thisBusyNode.runningJobs.filterNot(_.id == jobId)
        newNodeRunningJobs match {
          case job :: moreJobs =>
            (
              busyNodes.updated(
                agentId,
                thisBusyNode.copy(
                  runningJobs = NonEmptyList(job, moreJobs),
                  node = thisBusyNode.node.copy(lastUsedAt = Instant.now())
                )
              ),
              unusedNodes
            )
          case Nil =>
            (
              busyNodes.removed(agentId),
              unusedNodes.updated(agentId, thisBusyNode.node.copy(lastUsedAt = Instant.now()))
            )
        }
      case None =>
        (busyNodes, unusedNodes)
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

    mesosGateway.makeSchedulerCall(
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
    agentId: AgentID
  ) extends Message
  private case class OfferAcceptFailed(
    offerId: OfferID,
    agentId: AgentID,
    jobId: String,
    error: Throwable
  ) extends Message
  private case class WorkerIsDone(taskId: TaskID) extends Message
  private case object ClusterScaleTick extends Message
  private case class AllocationPerformed(allocationResults: NonEmptyList[AgentLaunchResult]) extends Message
  private case class AllocationFailed(error: Throwable, resources: NonEmptyList[Resources]) extends Message
  private case class DeallocationPerformed(deallocatedIds: NonEmptyList[DeallocationParams]) extends Message
  private case class DeallocationFailed(error: Throwable, nodeIds: NonEmptyList[DeallocationParams]) extends Message

  private type JobId = String
  private type NodeId = String
  private type Transition = State => Behavior[Message]

  private case class State(
    queue: Queue[Job],
    workers: Map[TaskID, Worker],
    jobIdIndex: Map[JobId, TaskID],
    busyNodes: Map[AgentID, BusyNode],
    unusedNodes: Map[AgentID, Node],
    nodeIdIndex: Map[NodeId, AgentID]
  )

  private case class Worker(actor: ActorRef[MesosJobActor.Message], jobId: JobId, agentId: AgentID)

  def apply(
    mesosStreamId: String,
    frameworkId: FrameworkID,
    mesosFrameworkActor: ActorRef[MesosFrameworkActor.Message],
    mesosGateway: MesosHttpGateway,
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
            Map.empty, // FIXME load from dao
            Map.empty // FIXME load from dao
          )
        )
      }
    }

}
