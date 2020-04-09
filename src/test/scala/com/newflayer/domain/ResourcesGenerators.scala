package com.newflayer.domain

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

trait ResourcesGenerators {
  implicit val arbResources: Arbitrary[Resources] = Arbitrary {
    for {
      mem <- arbitrary[Long]
      cpus <- arbitrary[Double]
      disk <- arbitrary[Long]
    } yield Resources(mem, cpus, disk)
  }
}
