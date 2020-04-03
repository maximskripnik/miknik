package com.newflayer.utils

import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import java.time.Instant
import cats.data.NonEmptyList

trait CommonGenerators {

  implicit val arbInstant: Arbitrary[Instant] = Arbitrary {
    for {
      millis <- Gen.chooseNum(Instant.MIN.getEpochSecond, Instant.MAX.getEpochSecond)
      nanos <- Gen.chooseNum(Instant.MIN.getNano, Instant.MAX.getNano)
    } yield Instant.ofEpochMilli(millis).plusNanos(nanos)
  }

  implicit def arbNonEmptyList[T: Arbitrary]: Arbitrary[NonEmptyList[T]] = Arbitrary {
    Gen.nonEmptyListOf(implicitly[Arbitrary[T]].arbitrary).map { list => NonEmptyList(list.head, list.tail) }
  }

}
