package com.newflayer.miknik.domain

import java.time.Instant

case class Node(id: String, ip: String, resources: Resources, createdAt: Instant, lastUsedAt: Instant)
