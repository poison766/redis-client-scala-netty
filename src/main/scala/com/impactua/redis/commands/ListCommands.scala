package com.impactua.redis.commands

import com.impactua.redis.BinaryConverter
import com.impactua.redis.commands.ClientCommands._
import com.impactua.redis.commands.Cmd._
import com.impactua.redis.commands.ListCommands._

import scala.concurrent.Future

/**
 * http://redis.io/commands#list
 */
private[redis] trait ListCommands extends ClientCommands {

  def rpushAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Rpush(key, conv.write(value) )).map(integerResultAsInt)

  def rpush[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Int = await { rpushAsync(key, value)(conv) }

  def lpushAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Lpush(key, conv.write(value) )).map(integerResultAsInt)

  def lpush[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Int = await {  lpushAsync(key, value)(conv) }

  def llenAsync[T](key: String): Future[Int] = r.send(Llen(key)).map(integerResultAsInt)
  def llen[T](key: String): Int = await {  llenAsync(key) }

  def lrangeAsync[T](key: String, start: Int, end: Int)(implicit conv: BinaryConverter[T]): Future[Seq[T]] =
    r.send(Lrange(key, start, end)).map(multiBulkDataResultToFilteredSeq(conv))

  def lrange[T](key: String, start: Int, end: Int)(implicit conv: BinaryConverter[T]): Seq[T] =
    await { lrangeAsync(key, start, end)(conv) }

  def ltrimAsync(key: String, start: Int, end: Int): Future[Boolean] =
    r.send(Ltrim(key, start, end)).map(okResultAsBoolean)

  def ltrim(key: String, start: Int, end: Int): Boolean = await { ltrimAsync(key, start, end) }

  def lindexAsync[T](key: String, idx: Int)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Lindex(key, idx)).map(bulkDataResultToOpt(conv))

  def lindex[T](key: String, idx: Int)(implicit conv: BinaryConverter[T]): Option[T] = await { lindexAsync(key, idx)(conv) }

  def lsetAsync[T](key: String, idx: Int, value: T)(implicit conv: BinaryConverter[T]): Future[Boolean] =
    r.send(Lset(key, idx, conv.write(value))).map(okResultAsBoolean)

  def lset[T](key: String, idx: Int, value: T)(implicit conv: BinaryConverter[T]): Boolean =
    await { lsetAsync(key, idx, value)(conv) }

  def lremAsync[T](key: String, count: Int, value: T)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Lrem(key, count, conv.write(value))).map(integerResultAsInt)

  def lrem[T](key: String, count: Int, value: T)(implicit conv: BinaryConverter[T]): Int =
    await { lremAsync(key, count, value)(conv) }

  def lpopAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Lpop(key)).map(bulkDataResultToOpt(conv))

  def lpop[T](key: String)(implicit conv: BinaryConverter[T]): Option[T] = await { lpopAsync(key)(conv) }

  def rpopAsync[T](key: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(Rpop(key)).map(bulkDataResultToOpt(conv))

  def rpop[T](key: String)(implicit conv: BinaryConverter[T]): Option[T] = await { rpopAsync(key)(conv) }

  def rpoplpushAsync[T](srcKey: String, destKey: String)(implicit conv: BinaryConverter[T]): Future[Option[T]] =
    r.send(RpopLpush(srcKey, destKey)).map(bulkDataResultToOpt(conv))

  def rpoplpush[T](srcKey: String, destKey: String)(implicit conv: BinaryConverter[T]): Option[T] =
    await { rpoplpushAsync(srcKey, destKey)(conv) }
}

object ListCommands {

  case class Rpush(key: String, v: Array[Byte]) extends Cmd {
    def asBin = Seq(RPUSH, key.getBytes(charset), v)
  }

  case class Lpush(key: String, v: Array[Byte]) extends Cmd {
    def asBin = Seq(LPUSH, key.getBytes(charset), v)
  }

  case class Llen(key: String) extends Cmd {
    def asBin = Seq(LLEN, key.getBytes(charset))
  }

  case class Lrange(key: String, start: Int, end: Int) extends Cmd {
    def asBin = Seq(LRANGE, key.getBytes(charset), start.toString.getBytes, end.toString.getBytes)
  }

  case class Ltrim(key: String, start: Int, end: Int) extends Cmd {
    def asBin = Seq(LTRIM, key.getBytes(charset), start.toString.getBytes, end.toString.getBytes)
  }

  case class Lindex(key: String, idx: Int) extends Cmd {
    def asBin = Seq(LINDEX, key.getBytes(charset), idx.toString.getBytes)
  }

  case class Lset(key: String, idx: Int, value: Array[Byte]) extends Cmd {
    def asBin = Seq(LSET, key.getBytes(charset), idx.toString.getBytes, value)
  }

  case class Lrem(key: String, count: Int, value: Array[Byte]) extends Cmd {
    def asBin = Seq(LREM, key.getBytes(charset), count.toString.getBytes, value)
  }

  case class Lpop(key: String) extends Cmd {
    def asBin = Seq(LPOP, key.getBytes(charset))
  }

  case class Rpop(key: String) extends Cmd {
    def asBin = Seq(RPOP, key.getBytes(charset))
  }

  case class RpopLpush(srcKey: String, destKey: String) extends Cmd {
    def asBin = Seq(RPOPLPUSH, srcKey.getBytes(charset), destKey.getBytes(charset))
  }

}
