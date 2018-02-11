package com.impactua.redis.commands

import java.nio.charset.Charset

import com.impactua.redis.commands.Cmd._

private[redis] object Cmd {

  val charset = Charset.forName("UTF-8")

  val SPACE = " ".getBytes
  val EOL = "\r\n".getBytes
  val STRING_START = "$".getBytes
  val ARRAY_START = "*".getBytes

  val NX = "NX".getBytes
  val XX = "XX".getBytes
  val EX = "EX".getBytes

  val WITHSCORES = "WITHSCORES".getBytes

  // set
  val SADD = "SADD".getBytes
  val SCARD = "SCARD".getBytes
  val SDIFF = "SDIFF".getBytes
  val SDIFFSTORE = "SDIFFSTORE".getBytes
  val SINTER = "SINTER".getBytes
  val SINTERSTORE = "SINTERSTORE".getBytes
  val SISMEMBER = "SISMEMBER".getBytes
  val SMEMBERS = "SMEMBERS".getBytes
  val SMOVE = "SMOVE".getBytes
  val SPOP = "SPOP".getBytes
  val SRANDMEMBER = "SRANDMEMBER".getBytes
  val SREM = "SREM".getBytes
  val SUNION = "SUNION".getBytes
  val SUNIONSTORE = "SUNIONSTORE".getBytes
  val SSCAN = "SSCAN".getBytes //TODO

  // hash
  val HDEL = "HDEL".getBytes
  val HEXISTS = "HEXISTS".getBytes
  val HGET = "HGET".getBytes
  val HGETALL = "HGETALL".getBytes
  val HINCRBY = "HINCRBY".getBytes
  val HINCRBYFLOAT = "HINCRBYFLOAT".getBytes
  val HKEYS = "HKEYS".getBytes
  val HLEN = "HLEN".getBytes
  val HMGET = "HMGET".getBytes
  val HMSET = "HMSET".getBytes
  val HSET = "HSET".getBytes
  val HSETNX = "HSETNX".getBytes
  val HVALS = "HVALS".getBytes
  val HSCAN = "HSCAN".getBytes //TODO
  val HSTRLEN = "HSTRLEN".getBytes

  // string
  val APPEND = "APPEND".getBytes
  val BITCOUNT = "BITCOUNT".getBytes // TODO
  val BITFIELD = "BITFIELD".getBytes // TODO
  val BITOP = "BITOP".getBytes // TODO
  val BITPOS = "BITPOS".getBytes // TODO
  val DECR = "DECR".getBytes
  val DECRBY = "DECRBY".getBytes
  val GET = "GET".getBytes
  val GETBIT = "GETBIT".getBytes // TODO
  val GETRANGE = "GETRANGE".getBytes
  val GETSET = "GETSET".getBytes
  val INCR = "INCR".getBytes
  val INCRBY = "INCRBY".getBytes
  val INCRBYFLOAT = "INCRBYFLOAT".getBytes //TODO
  val MGET = "MGET".getBytes
  val MSET = "MSET".getBytes
  val MSETNX = "MSETNX".getBytes
  val PSETEX = "PSETEX".getBytes //TODO
  val SET = "SET".getBytes
  val SETBIT = "SETBIT".getBytes //TODO
  val SETEX = "SETEX".getBytes //TODO
  val SETNX = "SETNX".getBytes //TODO
  val SETRANGE = "SETRANGE".getBytes //TODO
  val STRLEN = "STRLEN".getBytes //TODO

  // transactions
  val DISCARD = "DISCARD".getBytes
  val EXEC = "EXEC".getBytes
  val MULTI = "MULTI".getBytes
  val UNWATCH = "UNWATCH".getBytes
  val WATCH = "WATCH".getBytes

  // generic
  val DEL = "DEL".getBytes
  val DUMP = "DUMP".getBytes //TODO not used
  val EXISTS = "EXISTS".getBytes
  val EXPIRE = "EXPIRE".getBytes
  val EXPIREAT = "EXPIREAT".getBytes //not used
  val KEYS = "KEYS".getBytes
  val MIGRATE = "MIGRATE".getBytes //TODO
  val MOVE = "MOVE".getBytes //TODO
  val OBJECT = "OBJECT".getBytes // not used
  val PERSIST = "PERSIST".getBytes
  val PEXPIRE = "PEXPIRE".getBytes //TODO
  val PEXPIREAT = "PEXPIREAT".getBytes //not used
  val PTTL = "PTTL".getBytes //TODO
  val RANDOMKEY = "RANDOMKEY".getBytes //not used
  val RENAME = "RENAME".getBytes
  val RENAMENX = "RENAMENX".getBytes
  val RESTORE = "RESTORE".getBytes //not used
  val SCAN = "SCAN".getBytes // TODO:
  val SORT = "SORT".getBytes
  val TOUCH = "TOUCH".getBytes // TODO since 3.2.1
  val TTL = "TTL".getBytes
  val TYPE = "TYPE".getBytes
  val UNLINK = "UNLINK".getBytes // TODO since 4.0.0
  val WAIT = "WAIT".getBytes // TODO

  // list
  val BLPOP = "BLPOP".getBytes
  val BRPOP = "BRPOP".getBytes
  val BRPOPLPUSH = "BRPOPLPUSH".getBytes
  val LINDEX = "LINDEX".getBytes
  val LINSERT = "LINSERT".getBytes //TODO
  val LLEN = "LLEN".getBytes
  val LPOP = "LPOP".getBytes
  val LPUSH = "LPUSH".getBytes
  val LPUSHX = "LPUSHX".getBytes // TODO
  val LRANGE = "LRANGE".getBytes
  val LREM = "LREM".getBytes
  val LSET = "LSET".getBytes
  val LTRIM = "LTRIM".getBytes
  val RPOP = "RPOP".getBytes
  val RPOPLPUSH = "RPOPLPUSH".getBytes
  val RPUSH = "RPUSH".getBytes
  val RPUSHX = "RPUSHX".getBytes

  // server
  val BGREWRITEAOF = "BGREWRITEAOF".getBytes
  val BGSAVE = "BGSAVE".getBytes
  val CLIENT_KILL = Seq("CLIENT".getBytes, "KILL".getBytes)
  val CLIENT_LIST = Seq("CLIENT".getBytes, "LIST".getBytes)
  val CLIENT_GETNAME = Seq("CLIENT".getBytes, "GETNAME".getBytes)
  val CLIENT_PAUSE = Seq("CLIENT".getBytes, "PAUSE".getBytes)
  val CLIENT_SETNAME = Seq("CLIENT".getBytes, "SETNAME".getBytes)
  val CLUSTER_SLOTS = Seq("CLUSTER".getBytes, "SLOTS".getBytes)
  val COMMAND = "COMMAND".getBytes
  val COMMAND_COUNT = Seq("COMMAND".getBytes, "COUNT".getBytes)
  val COMMAND_GETKEYS = Seq("COMMAND".getBytes, "GETKEYS".getBytes)
  val COMMAND_INFO = Seq("COMMAND".getBytes, "INFO".getBytes)
  val CONFIG_GET = Seq("CONFIG".getBytes, "GET".getBytes)
  val CONFIG_REWRITE = Seq("CONFIG".getBytes, "REWRITE".getBytes)
  val CONFIG_SET = Seq("CONFIG".getBytes, "SET".getBytes)
  val CONFIG_RESETSTAT = Seq("CONFIG".getBytes, "RESETSTAT".getBytes)
  val DBSIZE = "DBSIZE".getBytes
  val DEBUG_OBJECT = Seq("DEBUG".getBytes, "OBJECT".getBytes)
  val DEBUG_SEGFAULT = Seq("DEBUG".getBytes, "SEGFAULT".getBytes)
  val FLUSHALL = "FLUSHALL".getBytes
  val FLUSHDB = "FLUSHDB".getBytes
  val INFO = "INFO".getBytes
  val LASTSAVE = "LASTSAVE".getBytes
  val MONITOR = "MONITOR".getBytes
  val ROLE = "ROLE".getBytes
  val SAVE = "SAVE".getBytes
  val SHUTDOWN = "SHUTDOWN".getBytes
  val SLAVEOF = "SLAVEOF".getBytes
  val SLOWLOG = "SLOWLOG".getBytes
  val SYNC = "SYNC".getBytes
  val TIME = "TIME".getBytes

  // sorted_set
  val ZADD = "ZADD".getBytes
  val ZCARD = "ZCARD".getBytes
  val ZCOUNT = "ZCOUNT".getBytes
  val ZINCRBY = "ZINCRBY".getBytes
  val ZINTERSTORE = "ZINTERSTORE".getBytes
  val ZLEXCOUNT = "ZLEXCOUNT".getBytes
  val ZRANGE = "ZRANGE".getBytes
  val ZRANGEBYLEX = "ZRANGEBYLEX".getBytes
  val ZREVRANGEBYLEX = "ZREVRANGEBYLEX".getBytes
  val ZRANGEBYSCORE = "ZRANGEBYSCORE".getBytes
  val ZRANK = "ZRANK".getBytes
  val ZREM = "ZREM".getBytes
  val ZREMRANGEBYLEX = "ZREMRANGEBYLEX".getBytes
  val ZREMRANGEBYRANK = "ZREMRANGEBYRANK".getBytes
  val ZREMRANGEBYSCORE = "ZREMRANGEBYSCORE".getBytes
  val ZREVRANGE = "ZREVRANGE".getBytes
  val ZREVRANGEBYSCORE = "ZREVRANGEBYSCORE".getBytes
  val ZREVRANK = "ZREVRANK".getBytes
  val ZSCORE = "ZSCORE".getBytes
  val ZUNIONSTORE = "ZUNIONSTORE".getBytes
  val ZSCAN = "ZSCAN".getBytes

  // connection
  val ECHO = "ECHO".getBytes
  val PING = "PING".getBytes
  val QUIT = "QUIT".getBytes
  val SELECT = "SELECT".getBytes
  val AUTH = "AUTH".getBytes
  val SWAPDB = "SWAPDB".getBytes //TODO: since redis 4.0

  // pubsub
  val PSUBSCRIBE = "PSUBSCRIBE".getBytes
  val PUBSUB = "PUBSUB".getBytes
  val PUBLISH = "PUBLISH".getBytes
  val PUNSUBSCRIBE = "PUNSUBSCRIBE".getBytes
  val SUBSCRIBE = "SUBSCRIBE".getBytes
  val UNSUBSCRIBE = "UNSUBSCRIBE".getBytes
  val UNSUBSCRIBEALL = "UNSUBSCRIBEALL".getBytes

  // scripting
  val EVAL = "EVAL".getBytes
  val EVALSHA = "EVALSHA".getBytes
  val SCRIPT_DEBUG = Seq("SCRIPT".getBytes, "DEBUG".getBytes) // TODO since 3.2.0
  val SCRIPT_EXISTS = Seq("SCRIPT".getBytes, "EXISTS".getBytes)
  val SCRIPT_FLUSH = Seq("SCRIPT".getBytes, "FLUSH".getBytes)
  val SCRIPT_KILL = Seq("SCRIPT".getBytes, "KILL".getBytes)
  val SCRIPT_LOAD = Seq("SCRIPT".getBytes, "LOAD".getBytes)

  // hyperloglog
  val PFADD = "PFADD".getBytes
  val PFCOUNT = "PFCOUNT".getBytes
  val PFMERGE = "PFMERGE".getBytes

  // Geo TODO
  val GEOADD = "GEOADD".getBytes
  val GEODIST = "GEODIST".getBytes
  val GEOHASH = "GEOHASH".getBytes
  val GAORADIUS = "GAORADIUS".getBytes
  val GEOPOS = "GEOPOS".getBytes
  val GEORADIUSBYMEMBER = "GEORADIUSBYMEMBER".getBytes
}


abstract class Cmd {
  def asBin: Seq[Array[Byte]]
}

sealed trait ArrayFlatten {
  implicit val flattener2 = (t: (Array[Byte], Array[Byte])) ⇒ t._1.toList ::: t._2.toList

  implicit val flattener3 = (t: (Array[Byte], Array[Byte], Array[Byte])) ⇒ t._1.toList ::: t._2.toList ::: t._3.toList
}

case class Exists(key: String) extends Cmd {
  def asBin = Seq(EXISTS, key.getBytes(charset))
}

case class Type(key: String) extends Cmd {
  def asBin = Seq(TYPE, key.getBytes(charset))
}

case class Del(keys: Seq[String]) extends Cmd {
  def asBin = if(keys.length > 1)
    DEL :: keys.toList.map(_.getBytes(charset))
  else Seq(DEL, keys.head.getBytes(charset))
}

case class Rename(key: String, newKey: String, nx: Boolean) extends Cmd {
  def asBin = Seq(if(nx) RENAMENX else RENAME, key.getBytes(charset), newKey.getBytes(charset))
}

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
    if(expTime != -1) seq = seq ++ Seq(EX, expTime.toString.getBytes)

    if(nx) {
      seq = seq :+ NX
    } else if(xx) {
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

case class Expire(key: String, seconds: Int) extends Cmd {
  def asBin = Seq(EXPIRE, key.getBytes(charset), seconds.toString.getBytes(charset))
}

case class Persist(key: String) extends Cmd {
  def asBin = Seq(PERSIST, key.getBytes(charset))
}

case class Ttl(key: String) extends Cmd {
  def asBin = Seq(TTL, key.getBytes(charset))
}

case class Keys(pattern: String) extends Cmd {
  def asBin = Seq(KEYS, pattern.getBytes(charset))
}

// utils
case class Ping() extends Cmd { def asBin = Seq(PING) }
case class Info() extends Cmd { def asBin = Seq(INFO) }
case class FlushAll() extends Cmd { def asBin = Seq(FLUSHALL) }
case class Auth(password: String) extends Cmd { def asBin = Seq(AUTH, password.getBytes(charset)) }
case class Select(db: Int) extends Cmd { def asBin = Seq(SELECT, db.toString.getBytes(charset)) }
