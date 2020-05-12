package com.newflayer.miknik.utils

import scala.concurrent.duration.FiniteDuration

import java.util.concurrent.TimeUnit

object DurationConverters {

  def toScalaDuration(duration: java.time.Duration): FiniteDuration =
    FiniteDuration(duration.toNanos(), TimeUnit.NANOSECONDS)

}
