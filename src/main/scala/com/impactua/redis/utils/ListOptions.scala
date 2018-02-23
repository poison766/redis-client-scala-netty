package com.impactua.redis.utils

/**
  *
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 13.02.2018.
  */
object ListOptions {

  object LinsertOptions {
    sealed abstract class DirectionOpts(name: String) {
      def asBin = name.getBytes
    }

    object Before extends DirectionOpts("BEFORE")

    object After extends DirectionOpts("AFTER")

  }

}
