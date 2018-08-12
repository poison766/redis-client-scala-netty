package com.impactua.redis.codecs

import java.util

import com.impactua.redis._
import com.impactua.redis.codecs.RedisArrayAgregatorDecoder.AggregateState
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder

import scala.collection.mutable.ArrayBuffer

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 13.03.2017.
  */
//ChannelUpstreamHandler
class RedisArrayAgregatorDecoder extends OneToOneDecoder {

  val queue: util.Deque[AggregateState] = new util.ArrayDeque[AggregateState]

  override def decode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef): AnyRef = {
    //nothing do -> decode
    //out.add(obj) -> obj
    println("!!!ArrayAgregator `" + msg + "`")
    msg.asInstanceOf[RedisMessage] match {
      case header: ArrayHeaderRedisMessage if header.length == 0 =>
        EmptyArrayRedisMessage

      case header: ArrayHeaderRedisMessage if header.length == -1 =>
        NullRedisMessage

      case ArrayHeaderRedisMessage(length) if queue.isEmpty =>
        queue.push(new AggregateState(length))
        null

      case ArrayHeaderRedisMessage(length) if !queue.isEmpty =>
        queue.push(new AggregateState(length))
        null

      case proxyMsg if queue.isEmpty =>
        proxyMsg

      case partMsg if !queue.isEmpty =>
        println("part message " + partMsg)
        var promMsg: RedisMessage = partMsg

        while (!queue.isEmpty) {
          val current = queue.peekFirst()
          current.add(promMsg)

          if (current.isFull) {
            promMsg = ArrayRedisMessage(current.msgs.toList)
            queue.pop()
          } else {
            return null
          }
        }
        println("prom messages:" + partMsg)
        promMsg
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
