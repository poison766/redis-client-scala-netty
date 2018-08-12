package com.impactua.redis.connections

import java.net.InetSocketAddress
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import com.impactua.redis.RedisException
import com.impactua.redis.codecs._
import com.impactua.redis.commands.Cmd
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder

import scala.concurrent.Future
import scala.util.Try

object Netty3RedisConnection {

  private[redis] val commandEncoder = new RedisCommandEncoder() // stateless
  private[redis] val cmdQueue = new ArrayBlockingQueue[(Netty3RedisConnection, ResultFuture)](2048)

  private[redis] val queueProcessor = new Runnable {
    override def run() = {
      while(true) {
        val (conn, f) = cmdQueue.take()
        try {
          if (conn.isOpen) {
            conn.enqueue(f)
          } else {
            conn.reconnect()
            f.promise.failure(new IllegalStateException("Channel closed, command: " + f.cmd))
          }
        } catch {
          case e: Exception =>
            f.promise.failure(e); conn.shutdown()
        }
      }
    }
  }

  new Thread(queueProcessor, "Queue Processor").start()
}

class Netty3RedisConnection(val host: String, val port: Int) extends RedisConnection {

  private[redis] val executor = Executors.newCachedThreadPool()
  private[redis] val channelFactory = new NioClientSocketChannelFactory(executor, executor)
  private[Netty3RedisConnection] val isRunning = new AtomicBoolean(true)
  private[Netty3RedisConnection] val isConnecting = new AtomicBoolean(false)
  private[Netty3RedisConnection] val clientBootstrap = createClientBootstrap
  private[Netty3RedisConnection] val opQueue =  new ArrayBlockingQueue[ResultFuture](1028)
  private[Netty3RedisConnection] val clientState = new AtomicReference[ConnectionState](NormalConnectionState(opQueue))


  private[Netty3RedisConnection] def createClientBootstrap = {
    val clientBootstrap = new ClientBootstrap(channelFactory)

    clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      override def getPipeline = {
        val p = Channels.pipeline
        //new DelimiterBasedFrameDecoder(512 * 1024 * 1024, false, Unpooled.wrappedBuffer("\r\n".getBytes))
        p.addLast("response_frame_delimeter", new DelimiterBasedFrameDecoder(512 * 1024 * 1024, false, ChannelBuffers.copiedBuffer("\r\n".getBytes)))
        p.addLast("response_decoder", new RedisResponseDecoder())
        p.addLast("response_array_agregator", new RedisArrayAgregatorDecoder())
        p.addLast("response_accumulator", new RedisResponseHandler(clientState))

        p.addLast("command_encoder",      Netty3RedisConnection.commandEncoder)
        p
      }
      // pipeline.addLast("response_array_agregator", new RedisArrayAgregatorDecoder())
      // pipeline.addLast("response_handler", new RedisResponseHandler(clientState))
    })

    clientBootstrap.setOption("tcpNoDelay", true)
    clientBootstrap.setOption("keepAlive", true)
    clientBootstrap.setOption("connectTimeoutMillis", 1000)
    clientBootstrap
  }

  private[Netty3RedisConnection] val channel = new AtomicReference(newChannel())

  def newChannel() = {
    isConnecting.set(true)
    val channelReady = new CountDownLatch(1)
    val future = clientBootstrap.connect(new InetSocketAddress(host, port))
    var channelInternal: Channel = null

    future.addListener(new ChannelFutureListener() {
      override def operationComplete(channelFuture: ChannelFuture) = {
        if (channelFuture.isSuccess) {
          channelInternal = channelFuture.getChannel
          channelReady.countDown()
        } else {
          channelReady.countDown()
          throw channelFuture.getCause
        }
      }
    })

    try {
      channelReady.await(1, TimeUnit.MINUTES)
      isConnecting.set(false)
    } catch {
      case iex: InterruptedException =>
        throw new RedisException("Interrupted while waiting for connection")
    }

    channelInternal
  }

  def reconnect() {
    if (isRunning.get() && !isConnecting.compareAndSet(false, true)) {
      new Thread("reconnection-thread") {
        override def run() {
          Try {
            channel.set(newChannel())
          }
        }
      }.start()

    }
  }

  def send(cmd: Cmd): Future[Result] = {
    if (!isRunning.get()) {
      throw new RedisException("Connection closed")
    }
    val f = ResultFuture(cmd)
    Netty3RedisConnection.cmdQueue.offer((this, f), 10, TimeUnit.SECONDS)
    f.future
  }

  def isOpen: Boolean = {
    val channelLocal = channel.get()
    isRunning.get() && channelLocal != null && channelLocal.isOpen
  }

  def shutdown() {
    if (isOpen) {
      isRunning.set(false)
      val channelClosed = new CountDownLatch(1)

      channel.get.close().addListener(new ChannelFutureListener() {
        override def operationComplete(channelFuture: ChannelFuture) = {
          channelClosed.countDown()
        }
      })

      try {
        channelClosed.await(1, TimeUnit.MINUTES)
      } catch {
        case _: InterruptedException =>
          throw new RedisException("Interrupted while waiting for connection close")
      }
    }
    channelFactory.releaseExternalResources()
  }

  private def enqueue(f: ResultFuture) {
    opQueue.offer(f, 10, TimeUnit.SECONDS)
    channel.get().write(f).addListener(ChannelFutureListener.CLOSE_ON_FAILURE)
  }
}
