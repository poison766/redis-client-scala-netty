package com.impactua.redis.commands

import com.impactua.redis.commands.ClientCommands._
import com.impactua.redis.commands.Cmd._
import com.impactua.redis.commands.GeoCommands.{GeoAdd, GeoDist, GeoHash, GeoPos}
import com.impactua.redis.{BinaryConverter, GeoUnit, RedisClient}

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

object GeoCommands {

  val stringConverter = BinaryConverter.StringConverter

  case class GeoAdd(key: String, values: Seq[(Array[Byte], Array[Byte], Array[Byte])]) extends Cmd {
    def asBin = Seq(GEOADD) ++ values.flatMap(a => Seq(a._1, a._2, a._3))
  }

  case class GeoDist(key: String, member1: String, member2: String, unit: String) extends Cmd {
    def asBin = if ("m".equals(unit)) {
      List(GEODIST, stringConverter.write(member1), stringConverter.write(member2))
    } else {
      List(GEODIST, stringConverter.write(member1), stringConverter.write(member2), stringConverter.write(unit))
    }
  }

  case class GeoHash(key: String, members: Seq[String]) extends Cmd {
    def asBin = GEOHASH :: stringConverter.write(key) :: members.map(stringConverter.write).toList
  }

  case class GeoPos(key: String, members: Seq[String]) extends Cmd {
    def asBin = GEOPOS :: stringConverter.write(key) :: members.map(stringConverter.write).toList
  }

  // TODO: case class GeoRadius extends Cmd { def asBin = GAORADIUS :: Nil }

  // TODO: case class GeoRadiusByMember extends Cmd { def asBin = GEORADIUSBYMEMBER :: Nil }
}
