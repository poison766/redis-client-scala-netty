package com.impactua.redis.utils

import collection.immutable.Set

case class ScanResult(cursor: Int, keys: Set[String])
