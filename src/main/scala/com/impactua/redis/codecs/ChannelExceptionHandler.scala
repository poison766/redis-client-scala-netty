package com.impactua.redis.codecs

import org.jboss.netty.channel.{ChannelHandlerContext, ExceptionEvent}

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 02.03.2017.
  */
private[codecs] trait ChannelExceptionHandler {
  def handleException(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    ctx.sendUpstream(e)
  }
}
