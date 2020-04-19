package com.newflayer.miknik.domain

import org.scalacheck.Arbitrary
import org.scalacheck.Gen._

trait ResourcesGenerators {
  implicit val arbResources: Arbitrary[Resources] = Arbitrary {
    for {
      cpus <- choose(0.1, 32.0)
      mem <- choose(1, 2 * 1024 * 1024) // 1MB - 2TB
      disk <- choose(1024, 100 * 1024 * 2014) // 1GB - 100TB
    } yield Resources(mem, cpus, disk)
  }
}
