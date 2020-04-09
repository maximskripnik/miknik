package com.newflayer.miknik.utils

import java.time.Instant

import cats.data.NonEmptyList
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

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

  val nonEmptyAlphaNumString: Arbitrary[String] = Arbitrary {
    arbNonEmptyList[Char](Arbitrary(Gen.alphaNumChar)).arbitrary.map(_.toList.mkString)
  }

}
