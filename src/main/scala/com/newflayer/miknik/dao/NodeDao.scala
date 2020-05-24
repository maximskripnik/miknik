package com.newflayer.miknik.dao

import com.newflayer.miknik.dao.NodeDao.MesosNode
import com.newflayer.miknik.dao.NodeDao.RunningJob
import com.newflayer.miknik.domain.Job
import com.newflayer.miknik.domain.JobStatus
import com.newflayer.miknik.domain.Node
import com.newflayer.miknik.domain.Resources

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import java.sql.Connection
import java.time.Instant

import cats.data.NonEmptyList
import io.circe.parser._
import org.apache.mesos.v1.Protos.AgentID
import org.apache.mesos.v1.Protos.TaskID

class NodeDao(connection: Connection) {

  val nodesTableName = "node"
  val jobsTableName = "job"

  def create(node: Node, agentId: AgentID)(implicit ec: ExecutionContext): Future[Node] = {
    val sql = s"insert into $nodesTableName " +
      s"(id, agent_id, ip, cpus, mem, disk, created_at, last_used_at) " +
      s"values (?, ?, ?, ?, ?, ?, ?, ?)"
    val statement = connection.prepareStatement(sql)
    statement.setString(1, node.id)
    statement.setString(2, agentId.getValue)
    statement.setString(3, node.ip)
    statement.setDouble(4, node.resources.cpus)
    statement.setLong(5, node.resources.mem)
    statement.setLong(6, node.resources.disk)
    statement.setString(7, node.createdAt.toString)
    statement.setString(8, node.lastUsedAt.toString)
    Future {
      statement.execute()
      connection.commit()
      node
    }
  }

  def list()(implicit ec: ExecutionContext): Future[List[MesosNode]] = {
    val sql = s"select * from $nodesTableName left join $jobsTableName " +
      s"on $nodesTableName.id = $jobsTableName.node_id and " +
      s"$jobsTableName.status in ('${JobDao.statusToString(JobStatus.Pending)}', '${JobDao.statusToString(JobStatus.Running)}') " +
      s"order by $nodesTableName.created_at, $nodesTableName.id"

    val statement = connection.prepareStatement(sql)

    Future(statement.executeQuery()).map { rs =>
      var nodes = List.empty[MesosNode]
      var currentNode: (Node, AgentID) = null
      var currentJobs: List[RunningJob] = List.empty

      def updateCurrentNode(): Unit =
        currentNode = Node(
          id = rs.getString(1),
          ip = rs.getString(3),
          resources = Resources(cpus = rs.getDouble(4), mem = rs.getLong(5), disk = rs.getLong(6)),
          createdAt = Instant.parse(rs.getString(7)),
          lastUsedAt = Instant.parse(rs.getString(8))
        ) -> AgentID.newBuilder.setValue(rs.getString(2)).build()

      def updateCurrentJobs(): Unit = {
        val jobId = rs.getString(9)
        if (jobId != null) {
          currentJobs = RunningJob(
            job = Job(
              id = jobId,
              dockerImage = rs.getString(12),
              resources = Resources(
                cpus = rs.getDouble(13),
                mem = rs.getLong(14),
                disk = rs.getLong(15)
              ),
              cmd = parse(rs.getString(16))
                .flatMap(_.as[List[String]])
                .getOrElse(throw new IllegalArgumentException),
              env = parse(rs.getString(17))
                .flatMap(_.as[Map[String, String]])
                .getOrElse(throw new IllegalArgumentException),
              status = JobDao.stringToStatus(rs.getString(18)),
              error = Option(rs.getString(19)),
              created = Instant.parse(rs.getString(20)),
              updated = Instant.parse(rs.getString(21)),
              completed = Option(rs.getString(22)).map(Instant.parse(_))
            ),
            taskId = TaskID.newBuilder().setValue(rs.getString(11)).build()
          ) :: currentJobs
        }
      }

      def updateCollectedNodes(): Unit = {
        val newNode = MesosNode(
          node = currentNode._1,
          agentId = currentNode._2,
          jobs = currentJobs.toList
        )
        nodes = newNode :: nodes
        currentJobs = List.empty
      }

      while (rs.next()) {
        if (currentNode == null) {
          updateCurrentNode()
          updateCurrentJobs()
        } else {
          if (rs.getString(1) == currentNode._1.id) {
            updateCurrentJobs()
          } else {
            updateCollectedNodes()
            updateCurrentNode()
            updateCurrentJobs()
          }
        }
      }

      if (currentNode != null) {
        updateCollectedNodes()
      }

      nodes.toList
    }
  }

  def deleteAll(nodeIds: NonEmptyList[String])(implicit ec: ExecutionContext): Future[Int] = {
    val sql = s"delete from $nodesTableName where id in ${nodeIds.toList.map(id => s"'$id'").mkString("(", ",", ")")}"
    val statement = connection.prepareStatement(sql)
    Future {
      val count = statement.executeUpdate()
      connection.commit()
      count
    }
  }

}

object NodeDao {

  case class RunningJob(job: Job, taskId: TaskID)

  case class MesosNode(node: Node, agentId: AgentID, jobs: List[RunningJob])

}
