package com.impactua.redis.commands

import com.impactua.redis.BinaryConverter
import com.impactua.redis.commands.ClientCommands._
import com.impactua.redis.commands.Cmd.{PFADD, PFCOUNT, PFMERGE, charset}
import com.impactua.redis.commands.HyperLogLogCommands.{PfAdd, PfCount, PfMerge}

import scala.concurrent.Future

/**
 * http://redis.io/commands#hyperloglog
 */
private[redis] trait HyperLogLogCommands extends ClientCommands {

  def pfaddAsync[T](key: String, fields: T*)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(PfAdd(key, fields.map(conv.write))).map(integerResultAsBoolean)

  def pfadd[T](key: String, fields: T*)(implicit conv: BinaryConverter[T]): Boolean = await { pfaddAsync(key, fields:_*)(conv) }

  def pfcountAsync(key: String): Future[Int] = r.send(PfCount(key)).map(integerResultAsInt)

  def pfcount(key: String): Int = await { pfcountAsync(key) }

  def pfmergeAsync(dst: String, keys: String*): Future[Boolean] = r.send(PfMerge(dst, keys)).map(okResultAsBoolean)

  def pfmerge(dst: String, keys: String*): Boolean = await { pfmergeAsync(dst, keys:_*) }

}

object HyperLogLogCommands {

  final val stringConverter = BinaryConverter.StringConverter

  case class PfAdd(key: String, values: Seq[Array[Byte]]) extends Cmd {
    def asBin = PFADD :: stringConverter.write(key) :: values.toList
  }

  case class PfCount(key: String) extends Cmd {
    def asBin = Seq(PFCOUNT, stringConverter.write(key))
  }

  case class PfMerge(dst: String, keys: Seq[String]) extends Cmd {
    def asBin = PFMERGE :: stringConverter.write(dst) :: keys.toList.map(stringConverter.write)
  }
}
