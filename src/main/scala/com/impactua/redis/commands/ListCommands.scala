package com.impactua.redis.commands

import com.impactua.redis.BinaryConverter
import com.impactua.redis.commands.ClientCommands._
import com.impactua.redis.commands.Cmd._
import com.impactua.redis.commands.ListCommands._
import com.impactua.redis.utils.ListOptions.LinsertOptions
import com.impactua.redis.utils.ListOptions.LinsertOptions.DirectionOpts

import scala.concurrent.Future

/**
 * http://redis.io/commands#list
 */
private[redis] trait ListCommands extends ClientCommands {

  def rpushAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Rpush(key, conv.write(value) )).map(integerResultAsInt)

  def rpush[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Int = await { rpushAsync(key, value)(conv) }

  def rpushxAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Rpushx(key, conv.write(value) )).map(integerResultAsInt)

  def rpushx[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Int = await { rpushxAsync(key, value)(conv) }

  def lpushAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Lpush(key, conv.write(value) )).map(integerResultAsInt)

  def lpush[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Int = await {  lpushAsync(key, value)(conv) }

  def lpushxAsync[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Lpushx(key, conv.write(value) )).map(integerResultAsInt)

  def lpushx[T](key: String, value: T)(implicit conv: BinaryConverter[T]): Int = await {  lpushxAsync(key, value)(conv) }

  def llenAsync(key: String): Future[Int] = r.send(Llen(key)).map(integerResultAsInt)

  def llen(key: String): Int = await {  llenAsync(key) }

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

  def linsertAsync[T](key: String, pivot: T, value: T, direction: DirectionOpts = LinsertOptions.Before)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Linsert(key, direction, conv.write(pivot), conv.write(value))).map(integerResultAsInt)

  def linsert[T](key: String, pivot: T, value: T, direction: DirectionOpts = LinsertOptions.Before)(implicit conv: BinaryConverter[T]): Int = await {
    linsertAsync(key, pivot, value, direction)
  }

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

  val stringConverter = BinaryConverter.StringConverter
  val intConverter = BinaryConverter.IntConverter

  case class Rpush(key: String, v: Array[Byte]) extends Cmd {
    def asBin = Seq(RPUSH, stringConverter.write(key), v)
  }

  case class Lpush(key: String, v: Array[Byte]) extends Cmd {
    def asBin = Seq(LPUSH, stringConverter.write(key), v)
  }

  case class Llen(key: String) extends Cmd {
    def asBin = Seq(LLEN, stringConverter.write(key))
  }

  case class Lrange(key: String, start: Int, end: Int) extends Cmd {
    def asBin = Seq(LRANGE, stringConverter.write(key), intConverter.write(start), intConverter.write(end))
  }

  case class Ltrim(key: String, start: Int, end: Int) extends Cmd {
    def asBin = Seq(LTRIM, stringConverter.write(key), intConverter.write(start), intConverter.write(end))
  }

  case class Lindex(key: String, idx: Int) extends Cmd {
    def asBin = Seq(LINDEX, stringConverter.write(key), intConverter.write(idx))
  }

  case class Lset(key: String, idx: Int, value: Array[Byte]) extends Cmd {
    def asBin = Seq(LSET, stringConverter.write(key), intConverter.write(idx), value)
  }

  case class Lrem(key: String, count: Int, value: Array[Byte]) extends Cmd {
    def asBin = Seq(LREM, stringConverter.write(key), intConverter.write(count), value)
  }

  case class Lpop(key: String) extends Cmd {
    def asBin = Seq(LPOP, stringConverter.write(key))
  }

  case class Rpop(key: String) extends Cmd {
    def asBin = Seq(RPOP, stringConverter.write(key))
  }

  case class RpopLpush(srcKey: String, destKey: String) extends Cmd {
    def asBin = Seq(RPOPLPUSH, stringConverter.write(srcKey), stringConverter.write(destKey))
  }

  case class Rpushx(key: String, v: Array[Byte]) extends Cmd {
    def asBin = Seq(RPUSHX, stringConverter.write(key), v)
  }

  case class Lpushx(key: String, v: Array[Byte]) extends Cmd {
    def asBin = Seq(LPUSHX, stringConverter.write(key), v)
  }

  case class Linsert(key: String, direction: DirectionOpts, pivot: Array[Byte], value: Array[Byte]) extends Cmd {
    def asBin = Seq(LINSERT, stringConverter.write(key), direction.asBin, pivot, value)
  }

}
