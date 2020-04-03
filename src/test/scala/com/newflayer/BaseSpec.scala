package com.newflayer

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.mockito.ArgumentMatchersSugar
import org.mockito.IdiomaticMockito
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.ExecutionContext
import com.newflayer.utils.CommonGenerators
import org.scalatest.EitherValues
import org.mockito.cats.IdiomaticMockitoCats

trait BaseSpec
  extends AnyWordSpec
  with Matchers
  with ArgumentMatchersSugar
  with IdiomaticMockito
  with IdiomaticMockitoCats
  with EitherValues
  with ScalaFutures
  with ScalaCheckDrivenPropertyChecks
  with CommonGenerators {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global
}
