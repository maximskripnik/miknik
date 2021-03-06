package com.newflayer.miknik.utils

import java.time.Instant

import cats.data.NonEmptyList
import cats.kernel.Order
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

trait CommonGenerators {

  implicit val arbInstant: Arbitrary[Instant] = Arbitrary {
    for {
      millis <- Gen.chooseNum(Instant.MIN.getEpochSecond, Instant.MAX.getEpochSecond)
      nanos <- Gen.chooseNum(Instant.MIN.getNano, Instant.MAX.getNano)
    } yield Instant.ofEpochMilli(millis).plusNanos(nanos)
  }

  val arbNonEmptyAlphaNumString: Arbitrary[String] = Arbitrary {
    arbNel[Char](Arbitrary(Gen.alphaNumChar)).arbitrary.map(_.toList.mkString)
  }

  implicit def arbNel[T: Arbitrary]: Arbitrary[NonEmptyList[T]] = Arbitrary {
    Gen.nonEmptyListOf(implicitly[Arbitrary[T]].arbitrary).map { list => NonEmptyList(list.head, list.tail) }
  }

  def arbNelDistinctBy[T: Arbitrary, Y: Order](f: T => Y): Arbitrary[NonEmptyList[T]] = Arbitrary {
    arbNel.arbitrary.map { nel => nel.distinct(Order.by(f)) }
  }

}
