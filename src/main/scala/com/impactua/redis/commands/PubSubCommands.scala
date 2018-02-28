package com.impactua.redis.commands

import com.impactua.redis.BinaryConverter
import com.impactua.redis.commands.ClientCommands._
import com.impactua.redis.commands.Cmd._
import com.impactua.redis.commands.PubSubCommands.{Publish, Subscribe, Unsubscribe, UnsubscribeAll}
import com.impactua.redis.connections._

import scala.concurrent.Future

/**
 * http://redis.io/commands#pubsub
 */
private[redis] trait PubSubCommands extends ClientCommands {

  def publishAsync[T](channel: String, message: T)(implicit conv: BinaryConverter[T]) =
    r.send(Publish(channel, conv.write(message))).map(integerResultAsInt)

  def publish[T](channel: String, message: T)(implicit conv: BinaryConverter[T]) = await(publishAsync(channel, message)(conv))

  def subscribeAsync[T](channels: String*)(block: (String, T) => Unit)(implicit conv: BinaryConverter[T]): Future[Seq[Int]] = {
    r.send(Subscribe(channels, { bulkResult =>
      val (channelBin, dataBin) = bulkResult.results match {
        case Seq(_, channelBin, messageBin) =>
          (channelBin, messageBin)
        case Seq(_, channelPatternBin, channelBin, messageBin) =>
          (channelBin, messageBin)
      }

      val channelName = channelBin.data.map(BinaryConverter.StringConverter.read).get

      try {
        block(channelName, dataBin.data.map(conv.read).get)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    })).map { multiBulkDataResultToFilteredSeq(BinaryConverter.IntConverter) }
  }

  def subscribe[T](channels: String*)(block: (String, T) => Unit)(implicit conv: BinaryConverter[T]): Seq[Int] =
    await(subscribeAsync(channels:_*)(block)(conv))

  def unsubscribeAsync(channels: String*): Future[Seq[Int]] =
    r.send(Unsubscribe(channels)).map(multiBulkDataResultToFilteredSeq(BinaryConverter.IntConverter))

  def unsubscribe(channels: String*) = await(unsubscribeAsync(channels:_*))

  def unsubscribe = await(r.send(UnsubscribeAll()))
}

object PubSubCommands {

  final val stringConverter = BinaryConverter.StringConverter

  case class Publish(channel: String, v: Array[Byte]) extends Cmd {
    def asBin = Seq(PUBLISH, stringConverter.write(channel), v)
  }

  case class Subscribe(channels: Seq[String], handler: MultiBulkDataResult => Unit) extends Cmd {
    def asBin =
      (if (hasPattern) PSUBSCRIBE else SUBSCRIBE) :: channels.toList.map(stringConverter.write)

    def hasPattern = channels.exists(s => s.contains("*") || s.contains("?"))
  }

  case class Unsubscribe(channels: Seq[String]) extends Cmd {
    def asBin =
      (if (hasPattern) PUNSUBSCRIBE else UNSUBSCRIBE) :: channels.toList.map(stringConverter.write)

    def hasPattern = channels.exists(s => s.contains("*") || s.contains("?"))
  }

  case class UnsubscribeAll() extends Cmd {
    def asBin = Seq(UNSUBSCRIBE)
  }

}
