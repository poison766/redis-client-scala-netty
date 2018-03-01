package com.impactua.redis.commands

import com.impactua.redis.commands.ClientCommands._
import com.impactua.redis.commands.Cmd._
import com.impactua.redis.commands.GenericCommands._
import com.impactua.redis.connections._
import com.impactua.redis.utils.ScanResult
import com.impactua.redis.{BinaryConverter, KeyType, RedisClient}

import scala.collection.Set
import scala.concurrent.Future

/**
  * http://redis.io/commands#generic
  * http://redis.io/commands#connection
  */
private[redis] trait GenericCommands extends ClientCommands {
  self: RedisClient =>

  def delAsync(key: String*): Future[Int] = r.send(Del(key)).map(integerResultAsInt)

  def del(key: String*): Int = await {
    delAsync(key: _*)
  }

  def existsAsync(key: String): Future[Boolean] = r.send(Exists(key)).map(integerResultAsBoolean)

  def exists(key: String): Boolean = await(existsAsync(key))

  def expireAsync(key: String, seconds: Int): Future[Boolean] = r.send(Expire(key, seconds)).map(integerResultAsBoolean)

  def expire(key: String, seconds: Int): Boolean = await {
    expireAsync(key, seconds)
  }

  def keysAsync(pattern: String): Future[Set[String]] =
    r.send(Keys(pattern)).map(multiBulkDataResultToSet(BinaryConverter.StringConverter))

  def keys(pattern: String): Set[String] = await(keysAsync(pattern))

  def keytypeAsync(key: String): Future[KeyType] = r.send(Type(key)).map {
    case SingleLineResult(s) => KeyType(s)
    case x => throw new IllegalStateException("Invalid response got from server: " + x)
  }

  def keytype(key: String): KeyType = await(keytypeAsync(key))

  def persistAsync(key: String): Future[Boolean] = r.send(Persist(key)).map(integerResultAsBoolean)

  def persist(key: String): Boolean = await {
    persistAsync(key)
  }

  def renameAsync(key: String, newKey: String, notExist: Boolean = true): Future[Boolean] =
    r.send(Rename(key, newKey, notExist)).map(integerResultAsBoolean)

  def rename(key: String, newKey: String, notExist: Boolean = true) = await {
    renameAsync(key, newKey, notExist)
  }


  def ttlAsync(key: String): Future[Int] = r.send(Ttl(key)).map(integerResultAsInt)

  def ttl(key: String): Int = await {
    ttlAsync(key)
  }

  def flushall = await {
    r.send(FlushAll())
  }

  def ping(): Boolean = await {
    r.send(Ping()).map {
      case SingleLineResult("PONG") => true
      case _ => false
    }
  }

  def pingAsync: Future[Boolean] = r.send(Ping()).map {
    case SingleLineResult("PONG") => true
    case _ => false
  }

  def info: Map[String, String] = await {
    r.send(Info()).map {
      case BulkDataResult(Some(data)) =>
        val info = BinaryConverter.StringConverter.read(data)
        info.split("\r\n").map(_.split(":")).map(x => x(0) -> x(1)).toMap
      case x => throw new IllegalStateException("Invalid response got from server: " + x)
    }
  }

  def auth(password: String): Boolean = await(authAsync(password))

  def authAsync(password: String): Future[Boolean] = r.send(Auth(password)).map(okResultAsBoolean)

  def select(db: Int): Boolean = await(r.send(Select(db)).map(okResultAsBoolean))

  def selectAsync(db: Int): Future[Boolean] = r.send(Select(db)).map(okResultAsBoolean)

  def scanAsync(cursor: Int, count: Int = 10, pattern: Option[String] = None): Future[ScanResult] = {
    r.send(Scan(cursor, count, pattern)).map(multiBulkResultToScanResult)
  }

  def scan(cursor: Int, count: Int = 10, pattern: Option[String] = None): ScanResult = await(scanAsync(cursor, count, pattern))

}

object GenericCommands {

  final val stringConverter = BinaryConverter.StringConverter
  final val intConverter = BinaryConverter.IntConverter

  case class Del(keys: Seq[String]) extends Cmd {
    def asBin = DEL :: keys.toList.map(stringConverter.write)
  }

  case class Ping() extends Cmd {
    def asBin = Seq(PING)
  }

  case class Info() extends Cmd {
    def asBin = Seq(INFO)
  }

  case class FlushAll() extends Cmd {
    def asBin = Seq(FLUSHALL)
  }

  case class Auth(password: String) extends Cmd {
    def asBin = Seq(AUTH, stringConverter.write(password))
  }

  case class Select(db: Int) extends Cmd {
    def asBin = Seq(SELECT, intConverter.write(db))
  }

  case class Expire(key: String, seconds: Int) extends Cmd {
    def asBin = Seq(EXPIRE, stringConverter.write(key), intConverter.write(seconds))
  }

  case class Persist(key: String) extends Cmd {
    def asBin = Seq(PERSIST, stringConverter.write(key))
  }

  case class Ttl(key: String) extends Cmd {
    def asBin = Seq(TTL, stringConverter.write(key))
  }

  case class Keys(pattern: String) extends Cmd {
    def asBin = Seq(KEYS, stringConverter.write(pattern))
  }

  case class Exists(key: String) extends Cmd {
    def asBin = Seq(EXISTS, stringConverter.write(key))
  }

  case class Type(key: String) extends Cmd {
    def asBin = Seq(TYPE, stringConverter.write(key))
  }

  case class Rename(key: String, newKey: String, nx: Boolean) extends Cmd {
    def asBin = Seq(if (nx) RENAMENX else RENAME, stringConverter.write(key), stringConverter.write(newKey))
  }

  case class Scan(cursor: Int, count: Int, pattern: Option[String]) extends Cmd {
    override def asBin: Seq[Array[Byte]] = {
      val scanMatch = pattern.map(pattern => Seq(MATCH, stringConverter.write(pattern))).getOrElse(Seq.empty[Array[Byte]])
      Seq(SCAN, intConverter.write(cursor)) ++ scanMatch ++ Seq(COUNT, intConverter.write(count))
    }
  }

}
