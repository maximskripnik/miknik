package com.newflayer.routes.contracts

import com.newflayer.domain.ListResult

import io.circe.Encoder
import io.circe.generic.semiauto._

case class ListResponse[T](
  items: List[T],
  count: Long
)

object ListResponse {

  implicit def encoder[A: Encoder]: Encoder[ListResponse[A]] = deriveEncoder

  def apply[A, B](mapItem: A => B)(listResult: ListResult[A]): ListResponse[B] =
    apply(listResult.items.map(mapItem), listResult.count)

}
