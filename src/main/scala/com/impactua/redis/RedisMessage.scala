package com.impactua.redis

import com.impactua.redis.connections.BulkDataResult
import io.netty.util.CharsetUtil

/**
  * @author Yaroslav Derman <yaroslav.derman@gmail.com>.
  *         created on 12.03.2017.
  */
sealed trait RedisMessage

trait SimpleMessage extends RedisMessage {
  def asOptBin: Option[Array[Byte]] = None
}

trait ComplexMessage extends RedisMessage {
  def asBulk: Seq[BulkDataResult]
}

case object NullRedisMessage extends SimpleMessage

case object EmptyArrayRedisMessage extends SimpleMessage

case class StringRedisMessage(content: String) extends SimpleMessage {
  override def asOptBin: Option[Array[Byte]] = Option(content).map(_.getBytes(CharsetUtil.UTF_8))
}

case class RawRedisMessage(content: Array[Byte]) extends SimpleMessage {
  override def toString: String = new String(content)

  override def asOptBin: Option[Array[Byte]] = Some(content)
}

case class IntRedisMessage(number: Int) extends SimpleMessage {
  override def asOptBin: Option[Array[Byte]] = Some(number.toString.getBytes)
}

case class ErrorRedisMessage(error: String) extends SimpleMessage {
  override def asOptBin: Option[Array[Byte]] = Option(error).map(_.getBytes(CharsetUtil.UTF_8))
}

case class ArrayHeaderRedisMessage(length: Int) extends SimpleMessage {
  override def asOptBin: Option[Array[Byte]] = None
}

case class ArrayRedisMessage(children: List[RedisMessage]) extends ComplexMessage {
  def asBulk: Seq[BulkDataResult] = children.flatMap {
    case s: SimpleMessage => Seq(BulkDataResult(s.asOptBin))
    case c: ComplexMessage => c.asBulk
  }
} 
