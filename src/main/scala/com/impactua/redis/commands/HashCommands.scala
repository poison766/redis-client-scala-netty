package com.impactua.redis.commands

import com.impactua.redis.BinaryConverter
import com.impactua.redis.commands.ClientCommands._
import com.impactua.redis.commands.Cmd._
import com.impactua.redis.commands.HashCommands._

import scala.concurrent.Future

/**
 * http://redis.io/commands#hash
 */
private[redis] trait HashCommands extends ClientCommands {

  def hsetAsync[T](key: String, field: String, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Hset(key, field, conv.write(value))).map(integerResultAsBoolean)

  def hset[T](key: String, field: String, value: T)(implicit conv: BinaryConverter[T]): Boolean = await { hsetAsync(key, field, value)(conv) }

  def hgetAsync[T](key: String, field: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Hget(key, field)).map(bulkDataResultToOpt(conv))

  def hget[T](key: String, field: String)(implicit conv: BinaryConverter[T]): Option[T] = await { hgetAsync(key, field)(conv) }

  def hmgetAsync[T](key: String, fields: String*)(implicit conv: BinaryConverter[T]): Future[Map[String,T]] =
    r.send(Hmget(key, fields)).map(multiBulkDataResultToMap(fields, conv))

  def hmget[T](key: String, fields: String*)(implicit conv: BinaryConverter[T]): Map[String,T] =
    await { hmgetAsync(key, fields: _*)(conv) }

  def hmsetAsync[T](key: String, kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Hmset(key, kvs.map{kv => kv._1 -> conv.write(kv._2)})).map(okResultAsBoolean)

  def hmset[T](key: String, kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Boolean = await { hmsetAsync(key, kvs: _*)(conv) }

  def hincrAsync(key: String, field: String, delta: Int = 1): Future[Int] =
    r.send(Hincrby(key, field, delta)).map(integerResultAsInt)

  def hincr(key: String, field: String, delta: Int = 1): Int = await { hincrAsync(key, field, delta) }

  def hexistsAsync(key: String, field: String): Future[Boolean] = r.send(Hexists(key, field)).map(integerResultAsBoolean)
  def hexists(key: String, field: String): Boolean = await { hexistsAsync(key, field) }

  def hdelAsync(key: String, field: String): Future[Boolean] = r.send(Hdel(key, field)).map(integerResultAsBoolean)
  def hdel(key: String, field: String): Boolean = await { hdelAsync(key, field) }

  def hlenAsync(key: String): Future[Int] = r.send(Hlen(key)).map(integerResultAsInt)
  def hlen(key: String): Int = await { hlenAsync(key) }

  def hkeysAsync(key: String): Future[Seq[String]] =
    r.send(Hkeys(key)).map(multiBulkDataResultToFilteredSeq(BinaryConverter.StringConverter))

  def hkeys(key: String): Seq[String] = await { hkeysAsync(key) }

  def hvalsAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Seq[T]] =
    r.send(Hvals(key)).map(multiBulkDataResultToFilteredSeq(conv))

  def hvals[T](key: String)(implicit conv: BinaryConverter[T]): Seq[T] = await { hvalsAsync(key)(conv) }

  def hgetallAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Map[String,T]] = r.send(Hgetall(key)).map(multiBulkDataResultToMap(BinaryConverter.StringConverter, conv))

  def hgetall[T](key: String)(implicit conv: BinaryConverter[T]): Map[String,T] = await { hgetallAsync(key)(conv) }

  def hstrlen[T](key: String, field: String): Int = await { hstrlenAsync(key, field)}

  def hstrlenAsync[T](key: String, field: String): Future[Int] = r.send(Hstrlen(key, field)).map(integerResultAsInt)

  def hsetnxAsync[T](key: String, field: String, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Hsetnx(key, field, conv.write(value))).map(integerResultAsBoolean)

  def hsetnx[T](key: String, field: String, value: T)(implicit conv: BinaryConverter[T]): Boolean = await { hsetnxAsync(key, field, value) }

  def hincrbyfloatAsync[T](key: String, field: String, delta: Double)(implicit conv: BinaryConverter[T]): Future[Double] =
    r.send(Hincrbyfloat(key, field, delta)).map(doubleResultAsDouble)

  def hincrbyfloat[T](key: String, field: String, delta: Double = 1.0)(implicit conv: BinaryConverter[T]): Double = await { hincrbyfloatAsync(key, field, delta)}

}

object HashCommands {

  case class Hset(key: String, field: String, value: Array[Byte]) extends Cmd {
    def asBin = Seq(HSET, key.getBytes(charset), field.getBytes(charset), value)
  }

  case class Hget(key: String, field: String) extends Cmd {
    def asBin = Seq(HGET, key.getBytes(charset), field.getBytes(charset))
  }

  case class Hmget(key: String, fields: Seq[String]) extends Cmd {
    def asBin = HMGET :: key.getBytes(charset) :: fields.toList.map {
      _.getBytes(charset)
    }
  }

  case class Hmset(key: String, kvs: Seq[(String, Array[Byte])]) extends Cmd {
    def asBin = HMSET :: key.getBytes :: kvs.toList.flatMap { kv => List(kv._1.getBytes(charset), kv._2) }
  }

  case class Hincrby(key: String, field: String, delta: Int) extends Cmd {
    def asBin = Seq(HINCRBY, key.getBytes(charset), field.getBytes(charset), delta.toString.getBytes)
  }

  case class Hexists(key: String, field: String) extends Cmd {
    def asBin = Seq(HEXISTS, key.getBytes(charset), field.getBytes(charset))
  }

  case class Hdel(key: String, field: String) extends Cmd {
    def asBin = Seq(HDEL, key.getBytes(charset), field.getBytes(charset))
  }

  case class Hlen(key: String) extends Cmd {
    def asBin = Seq(HLEN, key.getBytes(charset))
  }

  case class Hkeys(key: String) extends Cmd {
    def asBin = Seq(HKEYS, key.getBytes(charset))
  }

  case class Hvals(key: String) extends Cmd {
    def asBin = Seq(HVALS, key.getBytes(charset))
  }

  case class Hgetall(key: String) extends Cmd {
    def asBin = Seq(HGETALL, key.getBytes(charset))
  }

  case class Hstrlen(key: String, field: String) extends Cmd {
    def asBin = Seq(HSTRLEN, key.getBytes, field.getBytes)
  }

  case class Hsetnx(key: String, field: String, value: Array[Byte], nx: Boolean = false) extends Cmd {
    def asBin = Seq(if (nx) HSET else HSETNX, key.getBytes(charset), field.getBytes(charset), value)
  }

  case class Hincrbyfloat(key: String, field: String, delta: Double) extends Cmd {
    def asBin = Seq(HINCRBYFLOAT, key.getBytes(charset), field.getBytes(charset), delta.toString.getBytes)
  }
}
