package com.impactua.redis.codecs

import java.util.Date
import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicReference

import com.impactua.redis._
import com.impactua.redis.connections._
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

import scala.annotation.tailrec

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 02.03.2017.
  */
private[redis] class RedisResponseHandler(connStateRef: AtomicReference[ConnectionState]) extends SimpleChannelInboundHandler[RedisMessage] with ChannelExceptionHandler {

  final val BULK_NONE = BulkDataResult(None)
  final val EMPTY_MULTIBULK = MultiBulkDataResult(Nil)

  @tailrec
  private def fillErrors(queue: BlockingQueue[ResultFuture]): Unit = {
    queue.poll() match {
      case null =>
      case el =>
        el.fillWithFailure(ErrorResult("Interrupted while waiting for connection"))
        fillErrors(queue)
    }
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    // close all future with error
    super.channelInactive(ctx)
    //TODO: via atomic ?!
    val state = connStateRef.get()
    fillErrors(state.queue)
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: RedisMessage): Unit = {
    //println(new Date() +  "------------ response " + msg)
    msg match {
      case ErrorRedisMessage(error) => handleResult(ErrorResult(error))
      case StringRedisMessage(content) => handleResult(SingleLineResult(content))
      case IntRedisMessage(number) => handleResult(BulkDataResult(Some(number.toString.getBytes)))
      case RawRedisMessage(bytes) => handleResult(BulkDataResult(Option(bytes)))
      case EmptyArrayRedisMessage => handleResult(EMPTY_MULTIBULK)
      //TODO: Multibulk from multibulk
      case a: ArrayRedisMessage => handleResult(MultiBulkDataResult(a.asBulk))
      case NullRedisMessage => handleResult(BULK_NONE)
      case _ => throw new Exception("Unexpected error: " + msg)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    handleException(ctx, cause)
  }

  private def handleResult(r: Result) {
    try {
      //fill result
      val nextStateOpt = connStateRef.get().handle(r)

      for (nextState <- nextStateOpt) {
        connStateRef.set(nextState)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

}
