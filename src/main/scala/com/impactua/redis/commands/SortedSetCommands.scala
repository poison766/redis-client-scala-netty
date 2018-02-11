package com.impactua.redis.commands

import com.impactua.redis.BinaryConverter
import com.impactua.redis.commands.ClientCommands._
import com.impactua.redis.commands.Cmd._
import com.impactua.redis.commands.SortedSetCommands._
import com.impactua.redis.utils.Options.Limit
import com.impactua.redis.utils.SortedSetOptions.{Agregation, SumAgregation, ZaddOptions}

import scala.collection.Set
import scala.concurrent.Future

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 02.03.2017.
  */
private[redis] trait SortedSetCommands extends ClientCommands {

  def zaddAsync[T](key: String, opts: ZaddOptions, kvs: (Float, T)*)(implicit conv: BinaryConverter[T]): Future[Int] = {
    r.send(Zadd(key, kvs.map(els => els._1 -> conv.write(els._2)), opts)).map(integerResultAsInt)
  }

  def zadd[T](key: String, opts: ZaddOptions, kvs: (Float, T)*)(implicit conv: BinaryConverter[T]): Int = await {
    zaddAsync(key, opts, kvs: _*)(conv)
  }

  def zaddAsync[T](key: String, kvs: (Float, T)*)(implicit conv: BinaryConverter[T]): Future[Int] =
    r.send(Zadd(key, kvs.map(els => els._1 -> conv.write(els._2)), ZaddOptions())).map(integerResultAsInt)

  def zadd[T](key: String, kvs: (Float, T)*)(implicit conv: BinaryConverter[T]): Int = await {
    zaddAsync(key, kvs: _*)(conv)
  }

  def zcardAsync(key: String): Future[Int] = r.send(Zcard(key)).map(integerResultAsInt)

  def zcard(key: String): Int = await {
    zcardAsync(key)
  }

  def zcountAsync(key: String, min: Float, max: Float): Future[Int] = r.send(Zcount(key, min, max)).map(integerResultAsInt)

  def zcount[T](key: String, min: Float, max: Float): Int = await {
    zcountAsync(key, min, max)
  }

  def zincrbyAsync[T](key: String, increment: Float, member: T)(implicit conv: BinaryConverter[T]): Future[Float] = {
    r.send(Zincrby(key, increment, conv.write(member))).map(stringResultAsFloat)
  }

  def zincrby[T](key: String, increment: Float, member: T)(implicit conv: BinaryConverter[T]): Float = await {
    zincrbyAsync(key, increment, member)(conv)
  }

  def zinterstoreAsync = ???

  def zinterstore = ???

  def zlexcountAsync[T](key: String, min: T, max: T)(implicit conv: BinaryConverter[T]): Future[Int] = {
    r.send(Zlexcount(key, conv.write(min), conv.write(max))).map(integerResultAsInt)
  }

  def zlexcount[T](key: String, min: T, max: T)(implicit conv: BinaryConverter[T]): Int = await {
    zlexcountAsync(key, min, max)(conv)
  }

  def zrangeAsync[T](key: String, start: Int, stop: Int)(implicit conv: BinaryConverter[T]): Future[Set[T]] = {
    r.send(Zrange(key, start, stop, false)).map(multiBulkDataResultToLinkedSet(conv))
  }

  def zrange[T](key: String, start: Int, stop: Int)(implicit conv: BinaryConverter[T]): Set[T] = await {
    zrangeAsync(key, start, stop)(conv)
  }

  def zrangeWithScoresAsync[T](key: String, start: Int, stop: Int)(implicit conv: BinaryConverter[T]): Future[Map[T, Float]] = {
    r.send(Zrange(key, start, stop, true)).map(multiBulkDataResultToMap(conv, BinaryConverter.FloatConverter))
  }

  def zrangeWithScores[T](key: String, start: Int, stop: Int)(implicit conv: BinaryConverter[T]): Map[T, Float] = await {
    zrangeWithScoresAsync(key, start, stop)(conv)
  }

  def zrangeByLexAsync[T](key: String, min: String, max: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Future[Set[T]] = {
    r.send(ZrangeByLex(key, min, max, limit)).map(multiBulkDataResultToLinkedSet(conv))
  }

  def zrangeByLex[T](key: String, min: String, max: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Set[T] = await {
    zrangeByLexAsync(key, min, max, limit)(conv)
  }

  def zrangeByScoreAsync[T](key: String, min: String, max: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Future[Set[T]] = {
    r.send(ZrangeByScore(key, min, max, false, limit)).map(multiBulkDataResultToLinkedSet(conv))
  }

  def zrangeByScore[T](key: String, min: String, max: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Set[T] = await {
    zrangeByScoreAsync(key, min, max, limit)(conv)
  }

  def zrangeByScoreWithScoresAsync[T](key: String, min: String, max: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Future[Map[T, Float]] = {
    r.send(ZrangeByScore(key, min, max, true, limit)).map(multiBulkDataResultToMap(conv, BinaryConverter.FloatConverter))
  }

  def zrangeByScoreWithScores[T](key: String, min: String, max: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Map[T, Float] = await {
    zrangeByScoreWithScoresAsync(key, min, max, limit)(conv)
  }

  def zrankAsync[T](key: String, member: T)(implicit conv: BinaryConverter[T]): Future[Option[Int]] = {
    r.send(Zrank(key, conv.write(member))).map(bulkDataResultToOpt(BinaryConverter.IntConverter))
  }

  def zrank[T](key: String, member: T)(implicit conv: BinaryConverter[T]): Option[Int] = await {
    zrankAsync(key, member)(conv)
  }

  def zremAsync[T](key: String, members: T*)(implicit conv: BinaryConverter[T]): Future[Int] = {
    r.send(Zrem(key, members.map(conv.write))).map(integerResultAsInt)
  }

  def zrem[T](key: String, members: T*)(implicit conv: BinaryConverter[T]): Int = await {
    zremAsync(key, members:_*)(conv)
  }

  def zremRangeByLexAsync(key: String, min: String, max: String): Future[Int] = {
    r.send(ZremRangeByLex(key, min, max)).map(integerResultAsInt)
  }

  def zremRangeByLex(key: String, min: String, max: String): Int = await {
    zremRangeByLexAsync(key, min, max)
  }

  def zremRangeByRankAsync(key: String, startRange: Int, stopRange: Int): Future[Int] = {
    r.send(ZremRangeByRank(key, startRange, stopRange)).map(integerResultAsInt)
  }

  def zremRangeByRank(key: String, startRange: Int, stopRange: Int): Int = await {
    zremRangeByRankAsync(key, startRange, stopRange)
  }

  def zremRangeByScoreAsync(key: String, minScore: String, maxScore: String): Future[Int] = {
    r.send(ZremRangeByScore(key, minScore, maxScore)).map(integerResultAsInt)
  }

  def zremRangeByScore(key: String, minScore: String, maxScore: String): Int = await {
    zremRangeByScoreAsync(key, minScore, maxScore)
  }

  def zrevRangeAsync[T](key: String, start: Int, stop: Int)(implicit conv: BinaryConverter[T]): Future[Set[T]] = {
    r.send(ZrevRange(key, start, stop)).map(multiBulkDataResultToLinkedSet(conv))
  }

  def zrevRange[T](key: String, start: Int, stop: Int)(implicit conv: BinaryConverter[T]): Set[T] = await {
    zrevRangeAsync(key, start, stop)
  }

  def zrevRangeByLexAsync[T](key: String, start: String, stop: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Future[Set[T]] = {
    r.send(ZrevRangeByLex(key, start, stop, limit)).map(multiBulkDataResultToLinkedSet(conv))
  }

  def zrevRangeByLex[T](key: String, start: String, stop: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Set[T] = await {
    zrevRangeByLexAsync(key, start, stop, limit)
  }

  def zrevRangeByScoreAsync[T](key: String, start: String, stop: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Future[Set[T]] = {
    r.send(ZrevRangeByScore(key, start, stop, limit, false)).map(multiBulkDataResultToLinkedSet(conv))
  }

  def zrevRangeByScore[T](key: String, start: String, stop: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Set[T] = await {
    zrevRangeByScoreAsync(key, start, stop, limit)
  }

  def zrevRangeByScoreWithScoreAsync[T](key: String, start: String, stop: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Future[Map[T, Float]] = {
    r.send(ZrevRangeByScore(key, start, stop, limit, true)).map(multiBulkDataResultToMap(conv, BinaryConverter.FloatConverter))
  }

  def zrevRangeByScoreWithScore[T](key: String, start: String, stop: String, limit: Option[Limit] = None)(implicit conv: BinaryConverter[T]): Map[T, Float] = await {
    zrevRangeByScoreWithScoreAsync(key, start, stop, limit)(conv)
  }

  def zrevRankAsync[T](key: String, member: T)(implicit conv: BinaryConverter[T]): Future[Option[Int]] = {
    r.send(Zrevrank(key, conv.write(member))).map(bulkDataResultToOpt(BinaryConverter.IntConverter))
  }

  def zrevRank[T](key: String, member: T)(implicit conv: BinaryConverter[T]): Option[Int] = await {
    zrevRankAsync(key, member)(conv)
  }

  def zscoreAsync[T](key: String, member: T)(implicit conv: BinaryConverter[T]): Future[Option[Float]] = {
    r.send(Zscore(key, conv.write(member))).map(bulkDataResultToOpt(BinaryConverter.FloatConverter))
  }

  def zscore[T](key: String, member: T)(implicit conv: BinaryConverter[T]): Option[Float] = await {
    zscoreAsync(key, member)(conv)
  }

  def zunionstoreAsync(dstZsetName: String, zsetNumber: Int, srcZets: Seq[String], weights: Seq[Double] = Nil, agregationFunc: Agregation = SumAgregation): Future[Int] = {
    r.send(Zunionstore(dstZsetName, zsetNumber, srcZets, weights, agregationFunc)).map(integerResultAsInt)
  }

  def zunionstore(dstZsetName: String, zsetNumber: Int, srcZets: Seq[String], weights: Seq[Double] = Nil, agregationFunc: Agregation = SumAgregation): Int = await {
    zunionstoreAsync(dstZsetName, zsetNumber, srcZets, weights, agregationFunc)
  }

}

object SortedSetCommands {
  //sorted set
  case class Zadd(key: String, values: Seq[(Float, Array[Byte])], opts: ZaddOptions = ZaddOptions()) extends Cmd {
    def asBin = Seq(ZADD, key.getBytes(charset)) ++ opts.asBin ++ values.flatMap(kv => List(kv._1.toString.getBytes, kv._2))
  }

  case class Zcard(key: String) extends Cmd {
    def asBin = Seq(ZCARD, key.getBytes(charset))
  }

  case class Zcount(key: String, min: Float, max: Float) extends Cmd {
    def asBin = Seq(ZCOUNT, key.getBytes(charset), min.toString.getBytes(charset), max.toString.getBytes(charset))
  }

  case class Zincrby(key: String, increment: Float, member: Array[Byte]) extends Cmd {
    def asBin = Seq(ZINCRBY, key.getBytes(charset), increment.toString.getBytes(charset), member)
  }

  case class ZINTERSTORE(key: String) extends Cmd {
    def asBin = ???
  }

  case class Zlexcount(key: String, min: Array[Byte], max: Array[Byte]) extends Cmd {
    def asBin = Seq(ZLEXCOUNT, key.getBytes(charset), min, max)
  }

  case class Zrange(key: String, start: Int, stop: Int, withScores: Boolean) extends Cmd {
    def asBin = {
      val _withScores = if (withScores) Seq(WITHSCORES) else Nil
      Seq(ZRANGE, key.getBytes(charset), start.toString.getBytes(charset), stop.toString.getBytes(charset)) ++ _withScores
    }
  }

  case class ZrangeByLex(key: String, min: String, max: String, limit: Option[Limit]) extends Cmd {
    def asBin = {
      val withlimits = limit.map(_.asBin).getOrElse(Nil)
      Seq(ZRANGEBYLEX, key.getBytes(charset), min.toString.getBytes(charset), max.toString.getBytes(charset)) ++ withlimits
    }
  }

  case class ZrangeByScore(key: String, min: String, max: String, withScores: Boolean, limit: Option[Limit]) extends Cmd {
    def asBin = {
      val _withScores = if (withScores) Seq(WITHSCORES) else Nil
      val withlimits = limit.map(_.asBin).getOrElse(Nil)
      Seq(ZRANGEBYSCORE, key.getBytes(charset), min.getBytes(charset), max.getBytes(charset)) ++ _withScores ++ withlimits
    }
  }

  case class Zrank(key: String, member: Array[Byte]) extends Cmd {
    def asBin = {
      Seq(ZRANK, key.getBytes(charset), member)
    }
  }

  case class Zrem(key: String, members: Seq[Array[Byte]]) extends Cmd {
    def asBin = {
      Seq(ZREM, key.getBytes(charset)) ++ members
    }
  }

  case class ZremRangeByLex(key: String, min: String, max: String) extends Cmd {
    def asBin = {
      Seq(ZREMRANGEBYLEX, key.getBytes(charset), min.getBytes(charset), max.getBytes(charset))
    }
  }

  case class ZremRangeByRank(key: String, startRange: Int, stopRange: Int) extends Cmd {
    def asBin = {
      Seq(ZREMRANGEBYRANK, key.getBytes(charset), startRange.toString.getBytes(charset), stopRange.toString.getBytes(charset))
    }
  }

  case class ZremRangeByScore(key: String, minScore: String, maxScore: String) extends Cmd {
    def asBin = {
      Seq(ZREMRANGEBYSCORE, key.getBytes(charset), minScore.toString.getBytes(charset), maxScore.toString.getBytes(charset))
    }
  }

  case class ZrevRange(key: String, start: Int, stop: Int) extends Cmd {
    def asBin = {
      Seq(ZREVRANGE, key.getBytes(charset), start.toString.getBytes(charset), stop.toString.getBytes(charset))
    }
  }

  case class ZrevRangeByLex(key: String, min: String, max: String, limit: Option[Limit]) extends Cmd {
    def asBin = {
      val withlimits = limit.map(_.asBin).getOrElse(Nil)
      Seq(ZREVRANGEBYLEX, key.getBytes(charset), min.toString.getBytes(charset), max.toString.getBytes(charset)) ++ withlimits
    }
  }

  case class ZrevRangeByScore(key: String, min: String, max: String, limit: Option[Limit], withScores: Boolean) extends Cmd {
    def asBin = {
      val withlimits = limit.map(_.asBin).getOrElse(Nil)
      val _withScores = if (withScores) Seq(WITHSCORES) else Nil
      Seq(ZREVRANGEBYSCORE, key.getBytes(charset), min.toString.getBytes(charset), max.toString.getBytes(charset)) ++ _withScores ++ withlimits
    }
  }

  case class Zrevrank(key: String, member: Array[Byte]) extends Cmd {
    def asBin = {
      Seq(ZREVRANK, key.getBytes(charset), member)
    }
  }

  case class Zscore(key: String, member: Array[Byte]) extends Cmd {
    def asBin = {
      Seq(ZSCORE, key.getBytes(charset), member)
    }
  }

  case class Zunionstore(dstZsetName: String, zsetNumber: Int, srcZets: Seq[String], weights: Seq[Double], agregationFunc: Agregation = SumAgregation) extends Cmd {
    def asBin = {
      val _weights: Seq[Array[Byte]] = if (weights.isEmpty) Nil else Seq("WEIGHTS".getBytes(charset)) ++ weights.map(_.toString.getBytes(charset))

      Seq(ZUNIONSTORE, dstZsetName.getBytes(charset), zsetNumber.toString.getBytes(charset)) ++ srcZets.map(_.getBytes(charset)) ++
        _weights ++ Seq("AGGREGATE".getBytes(charset), agregationFunc.asBin)
    }
  }
}
