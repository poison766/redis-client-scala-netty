package com.impactua.redis.commands

import com.impactua.redis.commands.ClientCommands._
import com.impactua.redis.connections._
import com.impactua.redis.{BinaryConverter, GeoUnit, RedisClient}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * http://redis.io/commands#geo
 */
private[redis] trait GeoCommands extends ClientCommands {
  self: RedisClient =>

  def geoAddAsync(key: String, members: (Double, Double, String)* ): Future[Int] =
    r.send(GeoAdd(key, members.map {
      case (lat, lon, mem) => (
        BinaryConverter.DoubleConverter.write(lat),
        BinaryConverter.DoubleConverter.write(lon),
        BinaryConverter.StringConverter.write(mem)
      )
    })).map(integerResultAsInt)

  def geoAdd(key: String, members: (Double, Double, String)* ): Int = await { geoAddAsync(key, members:_*) }

  def geoDistAsync(key: String, member1: String, member2: String, unit: GeoUnit = GeoUnit.Meters): Future[Double] =
    r.send(GeoDist(key, unit.name, member1, member2)).map(doubleResultAsDouble)

  def geoDist(key: String, member1: String, member2: String, unit: GeoUnit = GeoUnit.Meters): Double =
    await { geoDistAsync(key, member1, member2, unit) }

  def geoHashAsync(key: String, members: String*): Future[Seq[String]] =
    r.send(GeoHash(key, members)).map(multiBulkDataResultToFilteredSeq(BinaryConverter.StringConverter))

  def geoHash(key: String, members: String*): Seq[String] = await { geoHashAsync(key, members:_*) }

  def geoPosAsync(key: String, members: String*): Future[Seq[String]] =
    r.send(GeoPos(key, members)).map(multiBulkDataResultToFilteredSeq(BinaryConverter.StringConverter))

  def geoPos(key: String, members: String*): Seq[String] = await { geoPosAsync(key, members:_*) }

}
