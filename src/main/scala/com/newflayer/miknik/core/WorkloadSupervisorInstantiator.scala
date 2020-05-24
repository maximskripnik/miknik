package com.newflayer.miknik.core

import com.newflayer.miknik.dao.JobDao
import com.newflayer.miknik.dao.NodeDao
import com.newflayer.miknik.domain.BusyNode
import com.newflayer.miknik.domain.JobStatus

import scala.collection.immutable.Nil
import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import cats.data.NonEmptyList
import org.apache.mesos.v1.Protos.FrameworkID

class WorkloadSupervisorInstantiator(
  nodeDao: NodeDao,
  jobDao: JobDao,
  mesosGateway: MesosHttpGateway,
  clusterChangeDecisionMaker: ClusterScaleDecisionMaker,
  mesosClusterManager: MesosClusterManager,
  clusterScaleTickPeriod: FiniteDuration
)(implicit ec: ExecutionContext) {

  def createBehaviour(
    mesosStreamId: String,
    frameworkId: FrameworkID,
    mesosFrameworkActor: ActorRef[MesosFrameworkActor.Message]
  ): Future[Behavior[WorkloadSupervisorActor.Message]] =
    for {
      nodes <- nodeDao.list()
      jobs <- jobDao.list(
        statuses = List(JobStatus.Pending),
        onlyWithoutNode = true,
        orderByCreated = true
      )
      initialState = WorkloadSupervisorActor.RecoveredState(
        Queue.from(jobs),
        Map.empty,
        Map.empty,
        Map.empty,
        Map.empty,
        Map.empty
      )
      state = nodes.foldLeft(initialState) { (state, mesosNode) =>
        val updatedNodeIdIndex = state.nodeIdIndex.updated(mesosNode.node.id, mesosNode.agentId)
        mesosNode.jobs match {
          case runningJobs @ runningJob :: moreRunningJobs =>
            val updatedBusyNodes = state.busyNodes.updated(
              mesosNode.agentId,
              BusyNode(mesosNode.node, NonEmptyList(runningJob, moreRunningJobs).map(_.job))
            )
            val (updatedJobIdIndex, updatedWorkersInfo) = runningJobs.foldLeft((state.jobIdIndex, state.workersInfo)) {
              case ((jobIdIndex, workersInfo), runningJob) =>
                (
                  jobIdIndex.updated(
                    runningJob.job.id,
                    runningJob.taskId
                  ),
                  workersInfo.updated(
                    runningJob.taskId,
                    WorkloadSupervisorActor.WorkerInfo(
                      MesosJobActor.apply(
                        runningJob.job.id,
                        jobDao,
                        mesosStreamId,
                        frameworkId,
                        mesosNode.agentId,
                        runningJob.taskId,
                        mesosGateway
                      ),
                      runningJob.job.id,
                      mesosNode.agentId
                    )
                  )
                )
            }
            state.copy(
              workersInfo = updatedWorkersInfo,
              jobIdIndex = updatedJobIdIndex,
              busyNodes = updatedBusyNodes
            )
          case Nil =>
            state.copy(
              unusedNodes = state.unusedNodes.updated(mesosNode.agentId, mesosNode.node),
              nodeIdIndex = updatedNodeIdIndex
            )
        }
      }
    } yield WorkloadSupervisorActor(
      mesosStreamId,
      frameworkId,
      mesosFrameworkActor,
      mesosGateway,
      jobDao,
      clusterChangeDecisionMaker,
      mesosClusterManager,
      clusterScaleTickPeriod,
      state
    )

}
