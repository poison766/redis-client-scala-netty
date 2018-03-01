package com.impactua.redis.utils

import collection.immutable.Set

case class ScanResult(cursor: Int, keys: Set[String])

case class HScanResult[K, V](cursor: Int, values: Map[K, V])
