package com.newflayer.domain

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen._
import java.time.Instant
import com.newflayer.utils.CommonGenerators
import scala.concurrent.duration.FiniteDuration

trait JobGenerators extends CommonGenerators with ResourcesGenerators {

  import JobStatus._

  implicit val arbJobStatus: Arbitrary[JobStatus] = Arbitrary(oneOf(Pending, Running, Completed, Failed, Canceled))

  implicit val arbJob: Arbitrary[Job] = Arbitrary {
    for {
      id <- alphaNumStr
      resources <- arbitrary[Resources]
      dockerImage <- alphaNumStr
      cmd <- listOf(alphaNumStr)
      env <- mapOf(alphaNumStr.flatMap(k => alphaNumStr.map((k, _))))
      status <- arbitrary[JobStatus]
      error <- option(alphaNumStr)
      created <- arbitrary[Instant]
      plusUpdated <- arbitrary[FiniteDuration]
      updated <- arbitrary[Instant].map(_.plusNanos(plusUpdated.toNanos))
    } yield Job(
      id = id,
      resources = resources,
      dockerImage = dockerImage,
      cmd = cmd,
      env = env,
      status = status,
      error = error,
      created = created,
      updated = updated
    )
  }

}
