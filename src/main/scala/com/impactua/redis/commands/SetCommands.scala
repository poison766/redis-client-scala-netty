package com.impactua.redis.commands

import com.impactua.redis.BinaryConverter
import com.impactua.redis.commands.ClientCommands._
import com.impactua.redis.commands.Cmd._
import com.impactua.redis.commands.SetCommands._

import scala.collection.Set
import scala.concurrent.Future

/**
 * http://redis.io/commands#set
 */
private[redis] trait SetCommands extends ClientCommands {

  def saddAsync[T](key: String, values: T*)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Sadd(key, values.map(conv.write))).map(integerResultAsInt)

  def sadd[T](key: String, values: T*)(implicit conv: BinaryConverter[T]): Int = await { saddAsync(key, values:_*)(conv) }

  def sremAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Srem(key, conv.write(value) )).map(integerResultAsBoolean)

  def srem[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Boolean = await { sremAsync(key,value)(conv) }

  def spopAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Spop(key)).map(bulkDataResultToOpt(conv))

  def spop[T](key: String)(implicit conv: BinaryConverter[T]): Option[T] = await { spopAsync(key)(conv) }

  def smoveAsync[T](srcKey: String, destKey: String, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Smove(srcKey, destKey, conv.write(value) )).map(integerResultAsBoolean)

  def smove[T](srcKey: String, destKey: String, value: T)(implicit conv: BinaryConverter[T]): Boolean =
    await { smoveAsync(srcKey, destKey, value)(conv) }

  def scardAsync(key: String): Future[Int] = r.send(Scard(key)).map(integerResultAsInt)
  def scard(key: String): Int = await { scardAsync(key) }

  def sismemberAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Sismember(key, conv.write(value) )).map(integerResultAsBoolean)

  def sismember[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Boolean =
    await { sismemberAsync(key, value)(conv) }

  def sinterAsync[T](keys: String*)(implicit conv: BinaryConverter[T]): Future[Set[T]] =
    r.send(Sinter(keys)).map(multiBulkDataResultToSet(conv))

  def sinter[T](keys: String*)(implicit conv: BinaryConverter[T]): Set[T] = await { sinterAsync(keys: _*)(conv) }

  def sinterstoreAsync[T](destKey: String, keys: String*): Future[Int] =
    r.send(Sinterstore(destKey, keys)).map(integerResultAsInt)

  def sinterstore[T](destKey: String, keys: String*): Int = await { sinterstoreAsync(destKey, keys: _*) }

  def sunionAsync[T](keys: String*)(implicit conv: BinaryConverter[T]): Future[Set[T]] =
    r.send(Sunion(keys)).map(multiBulkDataResultToSet(conv))

  def sunion[T](keys: String*)(implicit conv: BinaryConverter[T]): Set[T] = await { sunionAsync(keys: _*)(conv) }

  def sunionstoreAsync[T](destKey: String, keys: String*): Future[Int] =
    r.send(Sunionstore(destKey, keys)).map(integerResultAsInt)

  def sunionstore[T](destKey: String, keys: String*): Int = await { sunionstoreAsync(destKey, keys: _*) }

  def sdiffAsync[T](keys: String*)(implicit conv: BinaryConverter[T]): Future[Set[T]] =
    r.send(Sdiff(keys)).map(multiBulkDataResultToSet(conv))

  def sdiff[T](keys: String*)(implicit conv: BinaryConverter[T]): Set[T] = await { sdiffAsync(keys: _*)(conv) }

  def sdiffstoreAsync[T](destKey: String, keys: String*): Future[Int] =
    r.send(Sdiffstore(destKey, keys)).map(integerResultAsInt)

  def sdiffstore[T](destKey: String, keys: String*): Int = await { sdiffstoreAsync(destKey, keys: _*) }

  def smembersAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Set[T]] =
    r.send(Smembers(key)).map(multiBulkDataResultToSet(conv))

  def smembers[T](key: String)(implicit conv: BinaryConverter[T]): Set[T] = await { smembersAsync(key)(conv) }

  def srandmemberAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Srandmember(key)).map(bulkDataResultToOpt(conv))

  def srandmember[T](key: String)(implicit conv: BinaryConverter[T]): Option[T] = await { srandmemberAsync(key)(conv) }

}

object SetCommands {

  final val stringConverter = BinaryConverter.StringConverter

  case class Sadd(key: String, values: Seq[Array[Byte]]) extends Cmd {
    def asBin = Seq(SADD, stringConverter.write(key)) ++ values
  }

  case class Srem(key: String, v: Array[Byte]) extends Cmd {
    def asBin = Seq(SREM, stringConverter.write(key), v)
  }

  case class Spop(key: String) extends Cmd {
    def asBin = Seq(SPOP, stringConverter.write(key))
  }

  case class Smove(srcKey: String, destKey: String, value: Array[Byte]) extends Cmd {
    def asBin = Seq(SMOVE, stringConverter.write(srcKey), stringConverter.write(destKey), value)
  }

  case class Scard(key: String) extends Cmd {
    def asBin = Seq(SCARD, stringConverter.write(key))
  }

  case class Sismember(key: String, v: Array[Byte]) extends Cmd {
    def asBin = Seq(SISMEMBER, stringConverter.write(key), v)
  }

  case class Sinter(keys: Seq[String]) extends Cmd {
    def asBin = SINTER :: keys.toList.map(stringConverter.write)
  }

  case class Sinterstore(destKey: String, keys: Seq[String]) extends Cmd {
    def asBin = SINTERSTORE :: stringConverter.write(destKey) :: keys.toList.map(stringConverter.write)
  }

  case class Sunion(keys: Seq[String]) extends Cmd {
    def asBin = SUNION :: keys.toList.map(stringConverter.write)
  }

  case class Sunionstore(destKey: String, keys: Seq[String]) extends Cmd {
    def asBin = SUNIONSTORE :: stringConverter.write(destKey) :: keys.toList.map(stringConverter.write)
  }

  case class Sdiff(keys: Seq[String]) extends Cmd {
    def asBin = SDIFF :: keys.toList.map(stringConverter.write)
  }

  case class Sdiffstore(destKey: String, keys: Seq[String]) extends Cmd {
    def asBin = SDIFFSTORE :: stringConverter.write(destKey) :: keys.toList.map(stringConverter.write)
  }

  case class Smembers(key: String) extends Cmd {
    def asBin = Seq(SMEMBERS, stringConverter.write(key))
  }

  case class Srandmember(key: String) extends Cmd {
    def asBin = Seq(SRANDMEMBER, stringConverter.write(key))
  }

}
