package com.newflayer

import com.newflayer.utils.CommonGenerators

import scala.concurrent.ExecutionContext

import org.mockito.ArgumentMatchersSugar
import org.mockito.IdiomaticMockito
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.EitherValues
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

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
