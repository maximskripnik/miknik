package com.newflayer.routes.contracts

import com.newflayer.domain.{ Resources => DomainResources }

import io.circe._
import io.circe.generic.semiauto._
import io.scalaland.chimney.dsl._

case class Resources(
  mem: Long,
  cpus: Double,
  disk: Long
) {
  def toDomain: DomainResources = this.into[DomainResources].transform
}

object Resources {
  implicit val codec: Codec[Resources] = deriveCodec

  def apply(resources: DomainResources) = resources.into[Resources].transform
}
