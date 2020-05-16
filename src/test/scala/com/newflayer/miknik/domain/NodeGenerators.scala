package com.newflayer.miknik.domain

import com.newflayer.miknik.utils.CommonGenerators

import java.time.Instant

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen

trait NodeGenerators extends CommonGenerators with JobGenerators with ResourcesGenerators {

  implicit val arbIp: Arbitrary[String] = Arbitrary {
    for {
      p1 <- Gen.choose(0, 255)
      p2 <- Gen.choose(0, 255)
      p3 <- Gen.choose(0, 255)
      p4 <- Gen.choose(0, 255)
    } yield s"$p1.$p2.$p3.$p4"
  }

  implicit val arbNode: Arbitrary[Node] = Arbitrary {
    for {
      id <- arbNonEmptyAlphaNumString.arbitrary
      ip <- arbIp.arbitrary
      resources <- arbitrary[Resources]
      created <- arbitrary[Instant]
      lastUsed <- arbitrary[Instant]
    } yield Node(id, ip, resources, created, lastUsed)
  }

  implicit val arfBusyNode: Arbitrary[BusyNode] = Arbitrary {
    for {
      node <- arbitrary[Node]
      jobs <- arbJobNel.arbitrary
    } yield BusyNode(node, jobs)
  }

}
