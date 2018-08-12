package com.impactua.redis.codecs

import java.nio.charset.Charset
import java.util

import com.impactua.redis._
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.util.CharsetUtil

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 02.03.2017.
  */
private[redis] class RedisResponseDecoder extends ByteToMessageDecoder with ChannelExceptionHandler {

  val utf8: Charset = CharsetUtil.UTF_8
  var responseType: ResponseType = Unknown

  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]): Unit = {
    responseType match {
      case Unknown if in.isReadable =>
        responseType = ResponseType(in.readByte)

      case Unknown if !in.isReadable =>

      case BulkData =>
        readInt(in) match {
          case -1 =>
            responseType = Unknown
            out.add(NullRedisMessage)
          case n =>
            responseType = BinaryData(n)
        }

      case BinaryData(_) =>
        responseType = Unknown
        out.add(RawRedisMessage(readBytes(in)))

      case MultiBulkData =>
        responseType = Unknown
        out.add(ArrayHeaderRedisMessage(readInt(in)))

      case Integer =>
        responseType = Unknown
        out.add(IntRedisMessage(readInt(in)))

      case Error =>
        responseType = Unknown
        out.add(ErrorRedisMessage(readString(in)))

      case SingleLine =>
        responseType = Unknown
        out.add(StringRedisMessage(readString(in)))
    }
  }

  private def readInt(buf: ByteBuf): Int = readString(buf).toInt

  private def readString(buf: ByteBuf): String = new String(readBytes(buf), utf8)

  private def readBytes(buf: ByteBuf): Array[Byte] = {
    val bytes: Array[Byte] = new Array(buf.readableBytes())
    buf.readBytes(bytes)
    bytes
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    handleException(ctx, cause)
  }
}
