package com.impactua.redis.codecs

import java.util

import com.impactua.redis._
import com.impactua.redis.codecs.RedisArrayAgregatorDecoder.AggregateState
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageDecoder

import scala.collection.mutable.ArrayBuffer

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 13.03.2017.
  */
class RedisArrayAgregatorDecoder extends MessageToMessageDecoder[RedisMessage] {

  val queue: util.Deque[AggregateState] = new util.ArrayDeque[AggregateState]

  override def decode(ctx: ChannelHandlerContext, msg: RedisMessage, out: util.List[AnyRef]): Unit = {
    msg match {
      case ArrayHeaderRedisMessage(0) =>
        out.add(EmptyArrayRedisMessage)

      case ArrayHeaderRedisMessage(-1) =>
        out.add(NullRedisMessage)

      case ArrayHeaderRedisMessage(length) =>
        queue.push(new AggregateState(length))

      case proxyMsg if queue.isEmpty =>
        out.add(proxyMsg)

      case partMsg if !queue.isEmpty =>
        var promMsg: RedisMessage = partMsg

        while (!queue.isEmpty) {
          val current = queue.peekFirst()
          current.add(promMsg)

          if (current.isFull) {
            promMsg = ArrayRedisMessage(current.msgs.toList)
            queue.pop()
          } else {
            promMsg = null
            return
          }
        }

        Option(promMsg).foreach(msg => out.add(msg))
    }
  }

}

object RedisArrayAgregatorDecoder {

  case class AggregateState(length: Int, msgs: ArrayBuffer[RedisMessage]) {

    def isFull: Boolean = msgs.lengthCompare(length) == 0

    def this(length: Int) = this(length, new ArrayBuffer[RedisMessage](length))

    def add(msg: RedisMessage): Unit = msgs.append(msg)
  }

}
