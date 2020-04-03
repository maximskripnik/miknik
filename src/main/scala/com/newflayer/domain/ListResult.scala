package com.newflayer.domain

case class ListResult[T](items: List[T], count: Long)

object ListResult {

  def apply[T](items: List[T]): ListResult[T] = apply(items, items.size)

  def apply[T](items: T*): ListResult[T] = apply(items.toList)

}
