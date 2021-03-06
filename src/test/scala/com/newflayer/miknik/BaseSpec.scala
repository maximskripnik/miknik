package com.newflayer.miknik

import com.newflayer.miknik.utils.CommonGenerators

import scala.concurrent.ExecutionContext

import org.mockito.ArgumentMatchersSugar
import org.mockito.IdiomaticMockito
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

trait BaseSpec
  extends AnyWordSpecLike
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
