package com.impactua.redis.codecs

import com.impactua.redis.commands.Cmd._
import com.impactua.redis.connections.ResultFuture
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel.ChannelHandler.Sharable
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 02.03.2017.
  */
@Sharable
private[redis] class RedisCommandEncoder extends OneToOneEncoder {

  override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef): AnyRef = {
    val opFuture = msg.asInstanceOf[ResultFuture]
    println("Send command" + opFuture.cmd)
    binaryCmd(opFuture.cmd.asBin)
  }

  private def binaryCmd(cmdParts: Seq[Array[Byte]]): ChannelBuffer = {
    val params = new Array[Array[Byte]](3*cmdParts.length + 1)
    params(0) = ("*" + cmdParts.length + "\r\n").getBytes // num binary chunks
    var i = 1
    for(p <- cmdParts) {
      params(i) = ("$" + p.length + "\r\n").getBytes // len of the chunk
      i = i+1
      params(i) = p
      i = i+1
      params(i) = EOL
      i = i+1
    }
    ChannelBuffers.copiedBuffer(params: _*)
  }
}

//private[redis] class RedisCommandEncoder extends MessageToByteEncoder[ResultFuture] {
//
//  override def encode(ctx: ChannelHandlerContext, msg: ResultFuture, out: ByteBuf): Unit = {
//    binaryCmd(msg.cmd.asBin, out)
//  }
//
//  private def binaryCmd(cmdParts: Seq[Array[Byte]], out: ByteBuf) = {
//    out.writeBytes(ARRAY_START)
//    out.writeBytes(cmdParts.length.toString.getBytes)
//    out.writeBytes(EOL)
//
//    for (p <- cmdParts) {
//      out.writeBytes(STRING_START)
//      out.writeBytes(p.length.toString.getBytes)
//      out.writeBytes(EOL)
//      out.writeBytes(p)
//      out.writeBytes(EOL)
//    }
//  }
//}
