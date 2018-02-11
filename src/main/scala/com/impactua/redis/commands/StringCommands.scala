package com.impactua.redis.commands

import com.impactua.redis.BinaryConverter
import com.impactua.redis.commands.ClientCommands._
import com.impactua.redis.commands.Cmd._
import com.impactua.redis.commands.StringCommands._
import com.impactua.redis.connections._

import scala.concurrent.Future

/**
 * http://redis.io/commands#string
 */
private[redis] trait StringCommands extends ClientCommands {

  def appendAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Append(key, conv.write(value) )).map(integerResultAsInt)

  def append[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Int = await {appendAsync(key, value)(conv) }

  def decrAsync[T](key: String, delta: Int = 1): Future[Int] = r.send(Decr(key, delta)).map(integerResultAsInt)
  def decr[T](key: String, delta: Int = 1): Int = await { decrAsync(key, delta) }

  def setAsync[T](key: String, value: T, expTime: Int = -1)(implicit conv:BinaryConverter[T]): Future[Boolean] =
    r.send(SetCmd(key, conv.write(value), expTime)).map(okResultAsBoolean)

  def set[T](key: String, value: T, expTime: Int = -1)(implicit conv: BinaryConverter[T]): Boolean = await {
    setAsync(key, value, expTime)(conv)
  }

  def setNxAsync[T](key: String, value: T, expTime: Int = -1)(implicit conv:BinaryConverter[T]): Future[Boolean] =
    r.send(SetCmd(key, conv.write(value), expTime, true)).map(okResultAsBoolean)

  def setNx[T](key: String, value: T, expTime: Int = -1)(implicit conv: BinaryConverter[T]): Boolean = await {
    setNxAsync(key, value, expTime)(conv)
  }

  def setXxAsync[T](key: String, value: T, expTime: Int = -1)(implicit conv:BinaryConverter[T]): Future[Boolean] =
    r.send(SetCmd(key, conv.write(value), expTime, false, true)).map(okResultAsBoolean)

  def setXx[T](key: String, value: T, expTime: Int = -1)(implicit conv: BinaryConverter[T]): Boolean = await {
    setXxAsync(key, value, expTime)(conv)
  }

  def setNxAsync[T](kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(SetNx(kvs.map { kv => kv._1 -> conv.write(kv._2)})).map(integerResultAsBoolean)

  def setNx[T](kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Boolean = await { setNxAsync(kvs: _*)(conv) }

  def setAsync[T](kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(MSet(kvs.map { kv => kv._1 -> conv.write(kv._2)})).map(okResultAsBoolean)

  def set[T](kvs: (String,T)*)(implicit conv: BinaryConverter[T]): Boolean = await { setAsync(kvs: _*)(conv) }

  def getAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Get(key)).map(bulkDataResultToOpt(conv))

  def getAsync[T](keys: String*)(implicit conv: BinaryConverter[T]): Future[Seq[Option[T]]] = r.send(MGet(keys)).map {
    case BulkDataResult(data) => Seq(data.map(conv.read))
    case MultiBulkDataResult(results) => results.map { _.data.map(conv.read) }
    case x => throw new IllegalStateException("Invalid response got from server: " + x)
  }

  def get[T](key: String)(implicit conv: BinaryConverter[T]): Option[T] = await { getAsync(key)(conv) }
  def get[T](keys: String*)(implicit conv: BinaryConverter[T]): Seq[Option[T]] = await { getAsync(keys: _*)(conv) }

  def mgetAsync[T](keys: String*)(implicit conv: BinaryConverter[T]): Future[Map[String,T]] =
    r.send(MGet(keys)).map(multiBulkDataResultToMap(keys, conv))

  def mget[T](keys: String*)(implicit conv: BinaryConverter[T]): Map[String,T] = await { mgetAsync(keys: _*)(conv) }

  def getsetAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(GetSet(key, conv.write(value) )).map(bulkDataResultToOpt(conv))

  def getset[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Option[T] = await{ getsetAsync(key, value)(conv)}

  def incrAsync[T](key: String, delta: Int = 1): Future[Int] = r.send(Incr(key, delta)).map(integerResultAsInt)
  def incr[T](key: String, delta: Int = 1): Int = await { incrAsync(key, delta) }

  def getrangeAsync[T](key: String, startOffset: Int, endOffset: Int)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Getrange(key, startOffset, endOffset)).map(bulkDataResultToOpt(conv))

  def getrange[T](key: String, startOffset: Int, endOffset: Int)(implicit conv: BinaryConverter[T]): Option[T] =
    await { getrangeAsync(key, startOffset, endOffset)(conv) }
}

object StringCommands {
  case class Get(key: String) extends Cmd {
    def asBin = Seq(GET, key.getBytes(charset))
  }

  case class MGet(keys: Seq[String]) extends Cmd {
    def asBin = MGET :: keys.toList.map(_.getBytes(charset))
  }

  case class SetCmd(key: String,
                    v: Array[Byte],
                    expTime: Int,
                    nx: Boolean = false,
                    xx: Boolean = false) extends Cmd {

    def asBin = {
      var seq = Seq(SET, key.getBytes(charset), v)
      if (expTime != -1) seq = seq ++ Seq(EX, expTime.toString.getBytes)

      if (nx) {
        seq = seq :+ NX
      } else if (xx) {
        seq = seq :+ XX
      }

      seq
    }
  }

  case class MSet(kvs: Seq[(String, Array[Byte])]) extends Cmd {
    def asBin = MSET :: kvs.toList.flatMap { kv => List(kv._1.getBytes(charset), kv._2) }
  }

  case class SetNx(kvs: Seq[(String, Array[Byte])]) extends Cmd {
    def asBin = MSETNX :: kvs.toList.flatMap { kv => List(kv._1.getBytes(charset), kv._2) }
  }

  case class GetSet(key: String, v: Array[Byte]) extends Cmd {
    def asBin = Seq(GETSET, key.getBytes(charset), v)
  }

  case class Incr(key: String, delta: Int = 1) extends Cmd {
    def asBin = if(delta == 1) Seq(INCR, key.getBytes(charset))
    else Seq(INCRBY, key.getBytes(charset), delta.toString.getBytes)
  }

  case class Decr(key: String, delta: Int = 1) extends Cmd {
    def asBin = if(delta == 1) Seq(DECR, key.getBytes(charset))
    else Seq(DECRBY, key.getBytes(charset), delta.toString.getBytes)
  }

  case class Append(key: String, v: Array[Byte]) extends Cmd {
    def asBin = Seq(APPEND, key.getBytes(charset), v)
  }

  case class Getrange(key: String, startOffset: Int, endOffset: Int) extends Cmd {
    def asBin = Seq(GETRANGE, key.getBytes(charset), startOffset.toString.getBytes, endOffset.toString.getBytes)
  }
}
