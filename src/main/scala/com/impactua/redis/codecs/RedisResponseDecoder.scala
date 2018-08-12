package com.impactua.redis.codecs

import java.nio.charset.Charset

import com.impactua.redis._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBufferIndexFinder}
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

  val EOL_FINDER = new ChannelBufferIndexFinder() {
    override def find(buf: ChannelBuffer, pos: Int): Boolean = {
      buf.getByte(pos) == '\r' && (pos < buf.writerIndex - 1) && buf.getByte(pos + 1) == '\n'
    }
  }

  //TODO: split into frame without '/r/n'
  override def decode(ctx: ChannelHandlerContext, ch: Channel, buf: ChannelBuffer): AnyRef = {
    val copyOfBuff = buf.copy()
    println("`" + new String(copyOfBuff.array()) + "`")
    println(responseType)
    responseType match {
      case Unknown if buf.readable() =>
        responseType = ResponseType(buf.readByte)
        decode(ctx, ch, buf)

      case Unknown if !buf.readable => null // need more data

      case BulkData => readAsciiLine(buf) match {
        case null => null
        case line =>
          line.toInt match {
            case -1 =>
              responseType = Unknown
              NullRedisMessage
            case n =>
              responseType = BinaryData(n)
              decode(ctx, ch, buf)
          }
      }

      case BinaryData(len) =>
        if (buf.readableBytes >= (len + 2)) {
          // +2 for eol
          /*val line = buf.toString(buf.readerIndex, n - buf.readerIndex, charset)
        buf.skipBytes(line.length + 2)
        line*/
          responseType = Unknown
          val data = buf.readSlice(len)
          println("Binary data `" + new String(data.copy().array()) + "``")
          buf.skipBytes(2) // eol is there too
          RawRedisMessage(data.array())
        } else {
          null // need more data
        }

      case MultiBulkData =>
        readAsciiLine(buf) match {
          case null => null
          case line =>
            responseType = Unknown
            ArrayHeaderRedisMessage(line.toInt)
        }

      case Integer =>
        readAsciiLine(buf) match {
          case null => null
          case line =>
            responseType = Unknown
            IntRedisMessage(line.toInt)
        }

      case Error =>
        readAsciiLine(buf) match {
          case null => null
          case line =>
            responseType = Unknown
            ErrorRedisMessage(line)
        }

      case SingleLine =>
        readAsciiLine(buf) match {
          case null => null
          case line =>
            responseType = Unknown
            StringRedisMessage(line)
        }
    }
  }

  private def readAsciiLine(buf: ChannelBuffer): String = if (!buf.readable) null else {
    buf.indexOf(buf.readerIndex, buf.writerIndex, EOL_FINDER) match {
      case -1 => null
      case n =>
        val line = buf.toString(buf.readerIndex, n - buf.readerIndex, charset)
        buf.skipBytes(line.length + 2)
        line
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    handleException(ctx, e)
  }

}
