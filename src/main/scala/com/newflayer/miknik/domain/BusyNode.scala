package com.newflayer.miknik.domain

import cats.data.NonEmptyList

case class BusyNode(node: Node, runningJobs: NonEmptyList[Job])
