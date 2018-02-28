package com.impactua.redis.commands

import com.impactua.redis.commands.ClientCommands._
import com.impactua.redis.commands.Cmd._
import com.impactua.redis.commands.TransactionsCommands._
import com.impactua.redis.{BinaryConverter, RedisClient}

/**
  * http://redis.io/commands#transactions
  */
private[redis] trait TransactionsCommands extends ClientCommands {
  self: RedisClient =>

  def discardAsync() = r.send(Discard())
  def multiAsync() = r.send(Multi()).map(okResultAsBoolean)
  def execAsync() = r.send(Exec()).map(multiBulkDataResultToSet(BinaryConverter.StringConverter))
  def watchAsync(keys: String*) = r.send(Watch(keys))
  def unwatchAsync() = r.send(Unwatch())

  def withTransaction(block: RedisClient => Unit) = {
    multiAsync()
    try {
      block(self)
      execAsync()
    } catch {
      case e: Exception =>
        discardAsync()
        throw e
    }
  }

}

object TransactionsCommands {

  final val stringConverter = BinaryConverter.StringConverter

  case class Multi() extends Cmd {
    def asBin = Seq(MULTI)
  }

  case class Exec() extends Cmd {
    def asBin = Seq(EXEC)
  }

  case class Discard() extends Cmd {
    def asBin = Seq(DISCARD)
  }

  case class Watch(keys: Seq[String]) extends Cmd {
    def asBin = WATCH :: keys.map(stringConverter.write).toList
  }

  case class Unwatch() extends Cmd {
    def asBin = Seq(UNWATCH)
  }

}