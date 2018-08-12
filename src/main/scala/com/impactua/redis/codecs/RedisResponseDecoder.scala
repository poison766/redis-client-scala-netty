package com.impactua.redis.codecs

import java.nio.charset.Charset

import com.impactua.redis._
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{Channel, ChannelHandlerContext, ExceptionEvent}
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.util.CharsetUtil

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 02.03.2017.
  */
private[redis] class RedisResponseDecoder extends FrameDecoder with ChannelExceptionHandler {

  val charset: Charset = CharsetUtil.UTF_8
  var responseType: ResponseType = Unknown

  override def decode(ctx: ChannelHandlerContext, ch: Channel, buf: ChannelBuffer): AnyRef = {
    responseType match {
      case Unknown if buf.readable() =>
        responseType = ResponseType(buf.readByte)
        decode(ctx, ch, buf)

      case Unknown if !buf.readable => null // need more data

      case BulkData =>
        readInt(buf) match {
          case -1 =>
            responseType = Unknown
            NullRedisMessage
          case n =>
            responseType = BinaryData(n)
            null
        }

      case BinaryData(_) =>
        val data = buf.readBytes(buf.readableBytes())
        responseType = Unknown
        RawRedisMessage(data.array())

      case MultiBulkData =>
        responseType = Unknown
        ArrayHeaderRedisMessage(readInt(buf))

      case Integer =>
        responseType = Unknown
        IntRedisMessage(readInt(buf))

      case Error =>
        responseType = Unknown
        ErrorRedisMessage(readString(buf))

      case SingleLine =>
        responseType = Unknown
        StringRedisMessage(readString(buf))
    }
  }

  private def readInt(buf: ChannelBuffer) = readString(buf).toInt

  private def readString(buf: ChannelBuffer) = {
    val data = buf.readBytes(buf.readableBytes())
    data.toString(charset)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    handleException(ctx, e)
  }

}
