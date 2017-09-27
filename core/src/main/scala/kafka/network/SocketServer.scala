/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.io.IOException
import java.net._
import java.nio.channels._
import java.nio.channels.{Selector => NSelector}
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{ChannelBuilders, KafkaChannel, ListenerName, Selectable, Send, Selector => KSelector}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection._
import JavaConverters._
import scala.util.control.{ControlThrowable, NonFatal}

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
//整体生产流程：
//KafkaProducer创建ProducerRecord，将详细缓存懂啊RecordAccumulator
//Sender线程从RecordAccumulator中读取消息，放入KafkafChannel.send字段中等待发送，同时放入InFlightRequest队列中等待响应
//通过KSelector发送消息
//Processor使用KSelector读取请求暂存到stageRecieves中
//poll结束后，放到completeReceives中
//解析放入RequestChannel.requestQueue中
//Handler线程从requestQueue中读出请求处理
//处理响应放入responseQueue中
//Processor线程从responseQueue中读取响应放入inflightResponse队列中缓存
//消息发送后从缓存队列中删除
//
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time, val credentialProvider: CredentialProvider) extends Logging with KafkaMetricsGroup {

  //需要监听的host ip 端口
  private val endpoints = config.listeners.map(l => l.listenerName -> l).toMap
  //线程数
  private val numProcessorThreads = config.numNetworkThreads
  //缓存的最大请求个数
  private val maxQueuedRequests = config.queuedMaxRequests
  //线程总数
  private val totalProcessorThreads = numProcessorThreads * endpoints.size
  //每个ip上面能创建的最大连接数
  private val maxConnectionsPerIp = config.maxConnectionsPerIp
  //具体指定到某ip上最大的连接数
  private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides
  this.logIdent = "[Socket Server on Broker " + config.brokerId + "], "

  //processor线程与handler线程之间交换数据的队列
  //包含一个requestQueue队列和多个reponseQueue队列
  //每个processro线程都对应responseQueue
  //processor将请求放入requestQueue中
  //handler线程处理请求产生的响应放入对应的responseQueue中
  val requestChannel = new RequestChannel(totalProcessorThreads, maxQueuedRequests)

  //用于完成读取请求和写回响应操作
  private val processors = new Array[Processor](totalProcessorThreads)

  //接受客户端建立连接请求，创建socket连接并分配给Processor处理
  private[network] val acceptors = mutable.Map[EndPoint, Acceptor]()

  //控制每个ip上面最大连接数功能
  private var connectionQuotas: ConnectionQuotas = _

  /**
   * Start the socket server
    * 初始化
   */
  def startup() {
    this.synchronized {

      connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)
      //socket的sendBuffer大小
      val sendBufferSize = config.socketSendBufferBytes
      //socket的reciveBuffer大小
      val recvBufferSize = config.socketReceiveBufferBytes
      val brokerId = config.brokerId

      var processorBeginIndex = 0
      //遍历endpoint 封装了需要监听的ip端口
      config.listeners.foreach { endpoint =>
        val listenerName = endpoint.listenerName
        val securityProtocol = endpoint.securityProtocol
        val processorEndIndex = processorBeginIndex + numProcessorThreads

        //创建processor对象
        for (i <- processorBeginIndex until processorEndIndex)
          processors(i) = newProcessor(i, connectionQuotas, listenerName, securityProtocol)

        //创建acceptor。第五个参数指定了数组中和acceptor对象对应的processor对象，一个acceptor对应对个processor
        //acceptor用来处理接受所有新连接
        //acceptor对应多个handler线程，处理请求并产生相应给Processor，通过RequestChannel进行通信
        //processor用来处理，拥有自己的selector,从连接中读取请求和写回响应
        val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId,
          processors.slice(processorBeginIndex, processorEndIndex), connectionQuotas)
        acceptors.put(endpoint, acceptor)
        //创建acceptor线程并启动
        Utils.newThread(s"kafka-socket-acceptor-$listenerName-$securityProtocol-${endpoint.port}", acceptor, false).start()

        //主线程阻塞等待Acceptor线程启动完成
        acceptor.awaitStartup()

        processorBeginIndex = processorEndIndex
      }
    }

    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {
        private val ioWaitRatioMetricNames = processors.map { p =>
          metrics.metricName("io-wait-ratio", "socket-server-metrics", p.metricTags)
        }

        def value = ioWaitRatioMetricNames.map { metricName =>
          Option(metrics.metric(metricName)).fold(0.0)(_.value)
        }.sum / totalProcessorThreads
      }
    )

    info("Started " + acceptors.size + " acceptor threads")
  }

  // register the processor threads for notification of responses
  requestChannel.addResponseListener(id => processors(id).wakeup())

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down")
    this.synchronized {
      //关闭所有acceptor
      acceptors.values.foreach(_.shutdown)
      //关闭所有processor
      processors.foreach(_.shutdown)
    }
    info("Shutdown completed")
  }

  def boundPort(listenerName: ListenerName): Int = {
    try {
      acceptors(endpoints(listenerName)).serverChannel.socket.getLocalPort
    } catch {
      case e: Exception => throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  /* `protected` for test usage */
  protected[network] def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                      securityProtocol: SecurityProtocol): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      listenerName,
      securityProtocol,
      config,
      metrics,
      credentialProvider
    )
  }

  /* For test usage */
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  /* For test usage */
  private[network] def processor(index: Int): Processor = processors(index)

}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {
  //标志startup线程是否完成
  private val startupLatch = new CountDownLatch(1)

  // `shutdown()` is invoked before `startupComplete` and `shutdownComplete` if an exception is thrown in the constructor
  // (e.g. if the address is already in use). We want `shutdown` to proceed in such cases, so we first assign an open
  // latch and then replace it in `startupComplete()`.
  //标志当前线程shutdown是否完成
  @volatile private var shutdownLatch = new CountDownLatch(0)

  //标志当前线程是否存活
  private val alive = new AtomicBoolean(true)

  def wakeup(): Unit

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    alive.set(false)
    wakeup()
    //shutdown阻塞线程，等待唤醒
    shutdownLatch.await()
  }

  /**
   * Wait for the thread to completely start up
   */
  //阻塞等待启动完成
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete(): Unit = {
    // Replace the open latch with a closed one
    //启动完成，唤醒线程
    shutdownLatch = new CountDownLatch(1)
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
   */
  //标示关闭操作完成，唤醒阻塞线程
  protected def shutdownComplete(): Unit = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning: Boolean = alive.get

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   */
  def close(selector: KSelector, connectionId: String): Unit = {
    //找到指定连接
    val channel = selector.channel(connectionId)
    if (channel != null) {
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        //减少连接数
        connectionQuotas.dec(address)
      //关闭连接
      selector.close(connectionId)
    }
  }

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(channel: SocketChannel): Unit = {
    if (channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      //减少连接数
      connectionQuotas.dec(channel.socket.getInetAddress)
      swallowError(channel.socket().close())
      swallowError(channel.close())
    }
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 */
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              processors: Array[Processor],
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {
  //打开nioSelector
  private val nioSelector = NSelector.open()
  //创建ServerScoketChannel
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)

  this.synchronized {
    //同步，为其对应的每个processor都创建相应的线程并启动
    processors.foreach { processor =>
      Utils.newThread(s"kafka-network-thread-$brokerId-${endPoint.listenerName}-${endPoint.securityProtocol}-${processor.id}",
        processor, false).start()
    }
  }

  /**
   * Accept loop that checks for new connection attempts
    * 核心方法
   */
  def run() {
    //注册OP_ACCEPT事件
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    //标示当前线程启动操作完成
    startupComplete()
    try {
      var currentProcessor = 0
      while (isRunning) {
        try {
          //等待关注的事件
          val ready = nioSelector.select(500)
          if (ready > 0) {
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                iter.remove()
                if (key.isAcceptable)
                  //调用accept方法处理OP_ACCEPT事件
                  accept(key, processors(currentProcessor))
                else
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")

                // round robin to the next processor thread
                //轮训算法选择processor
                currentProcessor = (currentProcessor + 1) % processors.length
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket and selector.")
      swallowError(serverChannel.close())
      swallowError(nioSelector.close())
      shutdownComplete()
    }
  }

  /*
   * Create a server socket to listen for connections on.
   */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if(host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
      serverChannel.socket().setReceiveBufferSize(recvBufferSize)

    try {
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))
    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString, port, e.getMessage), e)
    }
    serverChannel
  }

  /*
   * Accept a new connection
   */
  def accept(key: SelectionKey, processor: Processor) {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    try {
      //增减记录连接数
      connectionQuotas.inc(socketChannel.socket().getInetAddress)
      //配置socketChannel
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)
      if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
        socketChannel.socket().setSendBufferSize(sendBufferSize)

      debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))
      //将socketChannel交给processor处理
      processor.accept(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)
    }
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = nioSelector.wakeup()

}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 */
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               listenerName: ListenerName,
                               securityProtocol: SecurityProtocol,
                               config: KafkaConfig,
                               metrics: Metrics,
                               credentialProvider: CredentialProvider) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort)
        }
      }
      case _ => None
    }
  }

  private case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort"
  }
  //由此processor处理的新建SocketChannel
  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  //保存未发送的响应，发送成功后清除
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()

  private[kafka] val metricTags = mutable.LinkedHashMap(
    "listener" -> listenerName.value,
    "networkProcessor" -> id.toString
  ).asJava

  newGauge("IdlePercent",
    new Gauge[Double] {
      def value = {
        Option(metrics.metric(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags))).fold(0.0)(_.value)
      }
    },
    // for compatibility, only add a networkProcessor tag to the Yammer Metrics alias (the equivalent Selector metric
    // also includes the listener name)
    Map("networkProcessor" -> id.toString)
  )
  //用来处理网络连接
  private val selector = new KSelector(
    maxRequestSize,
    connectionsMaxIdleMs,
    metrics,
    time,
    "socket-server",
    metricTags,
    false,
    true,
    ChannelBuilders.serverChannelBuilder(listenerName, securityProtocol, config, credentialProvider.credentialCache))

  override def run() {
    //标示Process初始化流程已经完成
    startupComplete()
    while (isRunning) {
      try {
        // setup any new connections that have been queued up
        //处理新建ScoketChannel
        configureNewConnections()

        // register any new responses for writing
        //获取RequestChannel中的ResponseQueue队列
        processNewResponses()

        //读取请求，并发送响应
        poll()

        //处理Kselector中的读取请求completedRecieves队列
        processCompletedReceives()

        //处理Kselector中的发送成功请求队列completedSend
        processCompletedSends()

        //处理Kselector中的断开连接请求队列disconnected
        processDisconnected()
      } catch {
        // We catch all the throwables here to prevent the processor thread from exiting. We do this because
        // letting a processor exit might cause a bigger impact on the broker. Usually the exceptions thrown would
        // be either associated with a specific socket channel or a bad request. We just ignore the bad socket channel
        // or request. This behavior might need to be reviewed if we see an exception that need the entire broker to stop.
        case e: ControlThrowable => throw e
        case e: Throwable =>
          error("Processor got uncaught exception.", e)
      }
    }

    debug("Closing selector - processor " + id)
    swallowError(closeAll())
    //一系列关闭操作
    shutdownComplete()
  }

  private def processNewResponses() {
    //获得对应的responseQueue中的响应
    var curr = requestChannel.receiveResponse(id)
    while (curr != null) {
      try {
        curr.responseAction match {

            //没有响应要发送给客户端
          case RequestChannel.NoOpAction =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            updateRequestMetrics(curr.request)
            trace("Socket server received empty response to send, registering for read: " + curr)
            val channelId = curr.request.connectionId
            //注册读事件继续读
            if (selector.channel(channelId) != null || selector.closingChannel(channelId) != null)
                selector.unmute(channelId)

            //需要发给客户端
          case RequestChannel.SendAction =>
            val responseSend = curr.responseSend.getOrElse(
              throw new IllegalStateException(s"responseSend must be defined for SendAction, response: $curr"))
            //调用selector.send ,并将响应放到inflightResponse队列中缓存
            sendResponse(curr, responseSend)
          case RequestChannel.CloseConnectionAction =>
            updateRequestMetrics(curr.request)
            trace("Closing socket connection actively according to the response code.")
            close(selector, curr.request.connectionId)
        }
      } finally {
        curr = requestChannel.receiveResponse(id)
      }
    }
  }

  /* `protected` for test usage */
  protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send) {
    trace(s"Socket server received response to send, registering for write and sending data: $response")
    val channel = selector.channel(responseSend.destination)
    // `channel` can be null if the selector closed the connection because it was idle for too long
    if (channel == null) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $id")
      response.request.updateRequestMetrics(0L)
    }
    else {
      selector.send(responseSend)
      inflightResponses += (response.request.connectionId -> response)
    }
  }

  private def poll() {
    //每次调用都会读取新的请求，发送成功的请求，以及断开连接的请求
    //放入其completedReceives,completedSends,disconnected
    try selector.poll(300)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        error(s"Closing processor $id due to illegal state or IO exception")
        swallow(closeAll())
        shutdownComplete()
        throw e
    }
  }

  private def processCompletedReceives() {
    //遍历
    selector.completedReceives.asScala.foreach { receive =>
      try {
        //获得请求对应的channel
        val openChannel = selector.channel(receive.source)
        // Only methods that are safe to call on a disconnected channel should be invoked on 'openOrClosingChannel'.
        val openOrClosingChannel = if (openChannel != null) openChannel else selector.closingChannel(receive.source)
        //创建channel对应的session对象
        val session = RequestChannel.Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, openOrClosingChannel.principal.getName), openOrClosingChannel.socketAddress)
        //封装成request
        val req = RequestChannel.Request(processor = id, connectionId = receive.source, session = session,
          buffer = receive.payload, startTimeNanos = time.nanoseconds,
          listenerName = listenerName, securityProtocol = securityProtocol)
        //放入队列中等待处理
        requestChannel.sendRequest(req)
        //取消注册OP_READ事件，连接不在读取数据
        selector.mute(receive.source)
      } catch {
        case e @ (_: InvalidRequestException | _: SchemaException) =>
          // note that even though we got an exception, we can assume that receive.source is valid. Issues with constructing a valid receive object were handled earlier
          error(s"Closing socket for ${receive.source} because of error", e)
          close(selector, receive.source)
      }
    }
  }

  private def processCompletedSends() {
    //遍历
    selector.completedSends.asScala.foreach { send =>
      //响应已经发送，从队列中删除
      val resp = inflightResponses.remove(send.destination).getOrElse {
        throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
      }
      updateRequestMetrics(resp.request)
      //注册OP_READ时间，允许此连接继续读取数据
      selector.unmute(send.destination)
    }
  }

  private def updateRequestMetrics(request: RequestChannel.Request) {
    val channel = selector.channel(request.connectionId)
    val openOrClosingChannel = if (channel != null) channel else selector.closingChannel(request.connectionId)
    val networkThreadTimeNanos = if (openOrClosingChannel != null) openOrClosingChannel.getAndResetNetworkThreadTimeNanos() else 0L
    request.updateRequestMetrics(networkThreadTimeNanos)
  }

  private def processDisconnected() {
    //循环
    selector.disconnected.keySet.asScala.foreach { connectionId =>
      val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
        throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
      }.remoteHost
      //从队列中删除
      inflightResponses.remove(connectionId).foreach(response => updateRequestMetrics(response.request))
      // the channel has been closed by the selector but the quotas still need to be updated
      //减少对应的连接数
      connectionQuotas.dec(InetAddress.getByName(remoteHost))
    }
  }

  /**
   * Queue up a new connection for reading
   */
  //Acceprot调用其accept方法，唤醒线程执行run方法
  def accept(socketChannel: SocketChannel) {
    newConnections.add(socketChannel)
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
   */
  private def configureNewConnections() {
    //遍历新创建队列
    while (!newConnections.isEmpty) {
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        val localHost = channel.socket().getLocalAddress.getHostAddress
        val localPort = channel.socket().getLocalPort
        val remoteHost = channel.socket().getInetAddress.getHostAddress
        val remotePort = channel.socket().getPort
        val connectionId = ConnectionId(localHost, localPort, remoteHost, remotePort).toString
        //方法中ScoketChannel被封装成KafkaChannel
        selector.register(connectionId, channel)
      } catch {
        // We explicitly catch all non fatal exceptions and close the socket to avoid a socket leak. The other
        // throwables will be caught in processor and logged as uncaught exceptions.
        case NonFatal(e) =>
          val remoteAddress = channel.getRemoteAddress
          // need to close the channel here to avoid a socket leak.
          close(channel)
          error(s"Processor $id closed connection from $remoteAddress", e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll() {
    selector.channels.asScala.foreach { channel =>
      close(selector, channel.id)
    }
    selector.close()
  }

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = selector.wakeup()

}

class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

  private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  private val counts = mutable.Map[InetAddress, Int]()

  def inc(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      val max = overrides.getOrElse(address, defaultMax)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  def dec(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
