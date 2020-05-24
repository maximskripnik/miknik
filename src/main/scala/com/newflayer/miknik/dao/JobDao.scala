package com.newflayer.miknik.dao

import com.newflayer.miknik.domain.Job
import com.newflayer.miknik.domain.JobStatus
import com.newflayer.miknik.domain.JobStatus._
import com.newflayer.miknik.domain.Resources

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import java.sql.Connection
import java.sql.ResultSet
import java.time.Instant

import cats.implicits._
import io.circe.parser._
import io.circe.syntax._
import org.apache.mesos.v1.Protos.TaskID

class JobDao(connection: Connection) {

  val jobsTableName = "job"

  def create(job: Job)(implicit ec: ExecutionContext): Future[Job] = {
    val sql = s"insert into $jobsTableName " +
      s"(id, docker_image, cpus, mem, disk, cmd, env, status, error, created, updated, completed) " +
      s"values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    val statement = connection.prepareStatement(sql)
    statement.setString(1, job.id)
    statement.setString(2, job.dockerImage)
    statement.setDouble(3, job.resources.cpus)
    statement.setLong(4, job.resources.mem)
    statement.setLong(5, job.resources.disk)
    statement.setString(6, job.cmd.asJson.noSpaces)
    statement.setString(7, job.env.asJson.noSpaces)
    statement.setString(8, JobDao.statusToString(job.status))
    statement.setString(9, job.error.orNull)
    statement.setString(10, job.created.toString)
    statement.setString(11, job.updated.toString)
    statement.setString(12, job.completed.map(_.toString).orNull)
    Future {
      statement.execute()
      connection.commit()
      job
    }
  }

  def get(jobId: String)(implicit ec: ExecutionContext): Future[Option[Job]] = {
    val sql = "select id, docker_image, cpus, mem, disk, cmd, env, status, error, created, updated, completed " +
      s"from $jobsTableName where id = ?"
    val statement = connection.prepareStatement(sql)
    statement.setString(1, jobId)
    Future(statement.executeQuery()).map(buildJob(_).headOption)
  }

  def list(
    statuses: List[JobStatus] = List.empty,
    onlyWithoutNode: Boolean = false,
    orderByCreated: Boolean = false
  )(
    implicit ec: ExecutionContext
  ): Future[List[Job]] = {

    def buildStatusInFilter(statuses: List[JobStatus]) =
      s"status in ${statuses.map(status => s"'${JobDao.statusToString(status)}'").mkString("(", ",", ")")}"

    def buildOnlyWithoutNodeFilter() = "node_id is null and task_id is null"

    val whereClause = (statuses.nonEmpty, onlyWithoutNode) match {
      case (false, false) => ""
      case (true, false) => s"where ${buildStatusInFilter(statuses)}"
      case (false, true) => s"where ${buildOnlyWithoutNodeFilter()}"
      case (true, true) => s"where ${buildStatusInFilter(statuses)} and ${buildOnlyWithoutNodeFilter()}"
    }

    val orderBy = if (orderByCreated) "created asc" else "updated desc"

    val sql = "select id, docker_image, cpus, mem, disk, cmd, env, status, error, created, updated, completed " +
      s"from $jobsTableName $whereClause order by $orderBy"
    val statement = connection.prepareStatement(sql)
    Future(statement.executeQuery()).map(buildJob(_))
  }

  def count()(implicit ec: ExecutionContext): Future[Int] = {
    val sql = "select count(*) from job"
    val statement = connection.prepareStatement(sql)
    Future(statement.executeQuery()).map { rs =>
      rs.next()
      rs.getInt(1)
    }
  }

  def update(
    jobId: String,
    status: Option[JobStatus] = None,
    nodeId: Option[String] = None,
    taskId: Option[TaskID] = None,
    completed: Option[Instant] = None,
    error: Option[String] = None
  )(
    implicit ec: ExecutionContext
  ): Future[Unit] = {

    val allUpdates = List(
      "status" -> status,
      "node_id" -> nodeId,
      "task_id" -> taskId,
      "completed" -> completed,
      "error" -> error
    ).collect {
      case (name, Some(value)) => name -> value
    }

    if (allUpdates.isEmpty) {
      ().pure[Future]
    } else {

      val updateSuffix = allUpdates
        .map {
          case (name, _) => s"$name = ?"
        }
        .mkString(", ")

      val sql = s"update $jobsTableName SET $updateSuffix where id = ?"
      val statement = connection.prepareStatement(sql)

      allUpdates.zip(LazyList.from(1)).map {
        case (update, index) =>
          update match {
            case ("status", status: JobStatus) => statement.setString(index, JobDao.statusToString(status))
            case ("node_id", nodeId: String) => statement.setString(index, nodeId)
            case ("task_id", taskId: TaskID) => statement.setString(index, taskId.getValue())
            case ("completed", completed: Instant) => statement.setString(index, completed.toString)
            case ("error", error: String) => statement.setString(index, error)
            case unknown => Future.failed(new IllegalArgumentException(unknown.toString()))
          }
      }
      statement.setString(allUpdates.size + 1, jobId)

      Future {
        statement.executeUpdate()
        connection.commit()
      }
    }
  }

  def delete(jobId: String)(implicit ec: ExecutionContext): Future[Boolean] = {
    val sql = s"delete from $jobsTableName where id = ?"
    val statement = connection.prepareStatement(sql)
    statement.setString(1, jobId)
    Future {
      val deleted = statement.executeUpdate()
      connection.commit()
      deleted > 0
    }
  }

  private def buildJob(rs: ResultSet): List[Job] =
    new Iterator[Job] {
      override def hasNext: Boolean = rs.next()
      override def next(): Job = Job(
        id = rs.getString(1),
        dockerImage = rs.getString(2),
        resources = Resources(cpus = rs.getDouble(3), mem = rs.getLong(4), disk = rs.getLong(5)),
        cmd = parse(rs.getString(6))
          .flatMap(_.as[List[String]])
          .getOrElse(throw new IllegalArgumentException),
        env = parse(rs.getString(7))
          .flatMap(_.as[Map[String, String]])
          .getOrElse(throw new IllegalArgumentException),
        status = JobDao.stringToStatus(rs.getString(8)),
        error = Option(rs.getString(9)),
        created = Instant.parse(rs.getString(10)),
        updated = Instant.parse(rs.getString(11)),
        completed = Option(rs.getString(12)).map(Instant.parse(_))
      )
    }.toList

}

object JobDao {

  def statusToString(status: JobStatus): String = status match {
    case Pending => "pending"
    case Running => "running"
    case Completed => "completed"
    case Failed => "failed"
    case Canceled => "canceled"
  }

  def stringToStatus(string: String): JobStatus = string match {
    case "pending" => Pending
    case "running" => Running
    case "completed" => Completed
    case "failed" => Failed
    case "canceled" => Canceled
    case _ => throw new IllegalArgumentException(string)
  }

}
