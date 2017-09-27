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
package kafka.controller

import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.Gauge
import kafka.admin.{AdminUtils, PreferredReplicaLeaderElectionCommand}
import kafka.api._
import kafka.cluster.Broker
import kafka.common.{TopicAndPartition, _}
import kafka.log.LogConfig
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.server._
import kafka.utils.ZkUtils._
import kafka.utils._
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener, IZkStateListener}
import org.apache.kafka.common.errors.{BrokerNotAvailableException, ControllerMovedException}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, StopReplicaResponse}
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.Watcher.Event.KeeperState

import scala.collection._
import scala.util.Try
//controller中使用的上下文信息
class ControllerContext(val zkUtils: ZkUtils) {
  val stats = new ControllerStats

  var controllerChannelManager: ControllerChannelManager = null //通过发送多种请求管理集群中其他broker

  var shuttingDownBrokerIds: mutable.Set[Int] = mutable.Set.empty//正在关闭的BrokerId集合
  var epoch: Int = KafkaController.InitialControllerEpoch - 1 //年代信息。每次重新选举新的Leader Controller，会加1
  var epochZkVersion: Int = KafkaController.InitialControllerEpochZkVersion - 1 //年代信息的zk版本
  var allTopics: Set[String] = Set.empty //整个集群中全部的Topic名称
  var partitionReplicaAssignment: mutable.Map[TopicAndPartition, Seq[Int]] = mutable.Map.empty //记录了每个分区的ar集合
  var partitionLeadershipInfo: mutable.Map[TopicAndPartition, LeaderIsrAndControllerEpoch] = mutable.Map.empty //记录了每个分区的leader副本所在的brokerid,isr集合，以及epoch等信息
  val partitionsBeingReassigned: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] = new mutable.HashMap //记录了正在重新分配副本的分区

  //记录了当前可用的broker集合
  private var liveBrokersUnderlying: Set[Broker] = Set.empty
  //记录了当前可用的BrokerId集合
  private var liveBrokerIdsUnderlying: Set[Int] = Set.empty

  // setter 根据Broker集合对liveBrokerIdsUnderlying、liveBrokersUnderlying进行更新
  def liveBrokers_=(brokers: Set[Broker]) {
    liveBrokersUnderlying = brokers
    liveBrokerIdsUnderlying = liveBrokersUnderlying.map(_.id)
  }

  // getter 从liveBrokersUnderlying和liveBrokerIdsUnderlying中排除shuttingDownBrokerIds集合
  def liveBrokers = liveBrokersUnderlying.filter(broker => !shuttingDownBrokerIds.contains(broker.id))
  def liveBrokerIds = liveBrokerIdsUnderlying -- shuttingDownBrokerIds

  def liveOrShuttingDownBrokerIds = liveBrokerIdsUnderlying
  def liveOrShuttingDownBrokers = liveBrokersUnderlying

  //获取在指定Broker中存在有副本的分区集合
  def partitionsOnBroker(brokerId: Int): Set[TopicAndPartition] = {
    partitionReplicaAssignment.collect {
      case (topicAndPartition, replicas) if replicas.contains(brokerId) => topicAndPartition
    }.toSet
  }

  //获取指定Broker集合中保存的所有副本
  def replicasOnBrokers(brokerIds: Set[Int]): Set[PartitionAndReplica] = {
    brokerIds.flatMap { brokerId =>
      partitionReplicaAssignment.collect {
        case (topicAndPartition, replicas) if replicas.contains(brokerId) =>
          new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, brokerId)
      }
    }.toSet
  }
  //获取指定Topic的所有副本
  def replicasForTopic(topic: String): Set[PartitionAndReplica] = {
    partitionReplicaAssignment
      .filter { case (topicAndPartition, _) => topicAndPartition.topic == topic }
      .flatMap { case (topicAndPartition, replicas) =>
        replicas.map { r =>
          new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, r)
        }
      }.toSet
  }
  //获取指定topic的所有分区
  def partitionsForTopic(topic: String): collection.Set[TopicAndPartition] =
    partitionReplicaAssignment.keySet.filter(topicAndPartition => topicAndPartition.topic == topic)

  //获取所有可用Broker中保存的副本
  def allLiveReplicas(): Set[PartitionAndReplica] = {
    replicasOnBrokers(liveBrokerIds)
  }

  //获取指定分区集合的副本
  def replicasForPartition(partitions: collection.Set[TopicAndPartition]): collection.Set[PartitionAndReplica] = {
    partitions.flatMap { p =>
      val replicas = partitionReplicaAssignment(p)
      replicas.map(r => new PartitionAndReplica(p.topic, p.partition, r))
    }
  }

  //删除指定的Topic
  def removeTopic(topic: String) = {
    partitionLeadershipInfo = partitionLeadershipInfo.filter{ case (topicAndPartition, _) => topicAndPartition.topic != topic }
    partitionReplicaAssignment = partitionReplicaAssignment.filter{ case (topicAndPartition, _) => topicAndPartition.topic != topic }
    allTopics -= topic
  }

}

//用来管理分区，选举，分区分配等操作
//自身为了保存高可靠性，也分leader/followerss
//选举由zk实现，每个broker启动的时候都会创建一个KafkaController对象，但是整个集群只有leader才能对外提供服务，其他都是follower
//只有leader在zk上面注册watche，防止出现脑裂羊群效应。
//封装了其他组件，对外提供了api接口
//
object KafkaController extends Logging {
  val stateChangeLogger = new StateChangeLogger("state.change.logger")
  val InitialControllerEpoch = 1
  val InitialControllerEpochZkVersion = 1

  case class StateChangeLogger(override val loggerName: String) extends Logging

  def parseControllerId(controllerInfoString: String): Int = {
    try {
      Json.parseFull(controllerInfoString) match {
        case Some(m) =>
          val controllerInfo = m.asInstanceOf[Map[String, Any]]
          controllerInfo("brokerid").asInstanceOf[Int]
        case None => throw new KafkaException("Failed to parse the controller info json [%s].".format(controllerInfoString))
      }
    } catch {
      case _: Throwable =>
        // It may be due to an incompatible controller register version
        warn("Failed to parse the controller info as json. "
          + "Probably this controller is still using the old format [%s] to store the broker id in zookeeper".format(controllerInfoString))
        try {
          controllerInfoString.toInt
        } catch {
          case t: Throwable => throw new KafkaException("Failed to parse the controller info: " + controllerInfoString + ". This is neither the new or the old format.", t)
        }
    }
  }
}

class KafkaController(val config: KafkaConfig, zkUtils: ZkUtils, time: Time, metrics: Metrics, threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {
  this.logIdent = "[Controller " + config.brokerId + "]: "
  private val stateChangeLogger = KafkaController.stateChangeLogger
  val controllerContext = new ControllerContext(zkUtils)
  val partitionStateMachine = new PartitionStateMachine(this) //用于维护分区状态的状态机
  val replicaStateMachine = new ReplicaStateMachine(this) //用于维护副本的状态机

  // have a separate scheduler for the controller to be able to start and stop independently of the kafka server
  // visible for testing
  private[controller] val kafkaScheduler = new KafkaScheduler(1)

  // visible for testing
  private[controller] val eventManager = new ControllerEventManager(controllerContext.stats.rateAndTimeMetrics,
    _ => updateMetrics())

  val topicDeletionManager = new TopicDeletionManager(this, eventManager)
  val offlinePartitionSelector = new OfflinePartitionLeaderSelector(controllerContext, config)
  private val reassignedPartitionLeaderSelector = new ReassignedPartitionLeaderSelector(controllerContext)
  private val preferredReplicaPartitionLeaderSelector = new PreferredReplicaPartitionLeaderSelector(controllerContext)
  private val controlledShutdownPartitionLeaderSelector = new ControlledShutdownLeaderSelector(controllerContext)
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(this) //在和Broker进行通信的时候，批量发送

  private val brokerChangeListener = new BrokerChangeListener(this, eventManager)
  private val topicChangeListener = new TopicChangeListener(this, eventManager)
  private val topicDeletionListener = new TopicDeletionListener(this, eventManager)
  private val partitionModificationsListeners: mutable.Map[String, PartitionModificationsListener] = mutable.Map.empty
  private val partitionReassignmentListener = new PartitionReassignmentListener(this, eventManager)
  private val preferredReplicaElectionListener = new PreferredReplicaElectionListener(this, eventManager)
  private val isrChangeNotificationListener = new IsrChangeNotificationListener(this, eventManager)

  @volatile private var activeControllerId = -1
  @volatile private var offlinePartitionCount = 0
  @volatile private var preferredReplicaImbalanceCount = 0

  newGauge(
    "ActiveControllerCount",
    new Gauge[Int] {
      def value = if (isActive) 1 else 0
    }
  )

  newGauge(
    "OfflinePartitionsCount",
    new Gauge[Int] {
      def value: Int = offlinePartitionCount
    }
  )

  newGauge(
    "PreferredReplicaImbalanceCount",
    new Gauge[Int] {
      def value: Int = preferredReplicaImbalanceCount
    }
  )

  newGauge(
    "ControllerState",
    new Gauge[Byte] {
      def value: Byte = state.value
    }
  )

  def epoch: Int = controllerContext.epoch

  def state: ControllerState = eventManager.state

  def clientId: String = {
    val controllerListener = config.listeners.find(_.listenerName == config.interBrokerListenerName).getOrElse(
      throw new IllegalArgumentException(s"No listener with name ${config.interBrokerListenerName} is configured."))
    "id_%d-host_%s-port_%d".format(config.brokerId, controllerListener.host, controllerListener.port)
  }

  /**
   * On clean shutdown, the controller first determines the partitions that the
   * shutting down broker leads, and moves leadership of those partitions to another broker
   * that is in that partition's ISR.
   *
   * @param id Id of the broker to shutdown.
   * @return The number of partitions that the broker still leads.
   */
  // 主动下线Broker
  def shutdownBroker(id: Int, controlledShutdownCallback: Try[Set[TopicAndPartition]] => Unit): Unit = {
    val controlledShutdownEvent = ControlledShutdown(id, controlledShutdownCallback)
    eventManager.put(controlledShutdownEvent)
  }

  /**
   * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
   * It does the following things on the become-controller state change -
   * 1. Register controller epoch changed listener
   * 2. Increments the controller epoch
   * 3. Initializes the controller's context object that holds cache objects for current topics, live brokers and
   *    leaders for all existing partitions.
   * 4. Starts the controller's channel manager
   * 5. Starts the replica state machine
   * 6. Starts the partition state machine
   * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
   * This ensures another controller election will be triggered and there will always be an actively serving controller
   */
  def onControllerFailover() {
    info("Broker %d starting become controller state transition".format(config.brokerId))
    // 读取zk中的记录的ControllerEpochPath信息并更新到ControllerContext中
    readControllerEpochFromZookeeper()

    // 递增ControllerEpochPath并写入zk中
    incrementControllerEpoch()

    // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
    // 注册各种监听器
    registerPartitionReassignmentListener()
    registerIsrChangeNotificationListener()
    registerPreferredReplicaElectionListener()
    registerTopicChangeListener()
    registerTopicDeletionListener()
    registerBrokerChangeListener()

    // 初始化ControllerContext，从zk中读取topic,分区，副本相关的各种元数据信息，并启动ControllerChannleManager、TopicDeletetionManager等依赖组件
    initializeControllerContext()
    val (topicsToBeDeleted, topicsIneligibleForDeletion) = fetchTopicDeletionsInProgress()
    topicDeletionManager.init(topicsToBeDeleted, topicsIneligibleForDeletion)

    // We need to send UpdateMetadataRequest after the controller context is initialized and before the state machines
    // are started. The is because brokers need to receive the list of live brokers from UpdateMetadataRequest before
    // they can process the LeaderAndIsrRequests that are generated by replicaStateMachine.startup() and
    // partitionStateMachine.startup().
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)

    // 启动ReplicaStateMachine组件，初始化各个副本的状态
    replicaStateMachine.startup()

    // 启动ParitionStateMachine组件，其中会初始化各个分区的状态
    partitionStateMachine.startup()

    // register the partition change listeners for all existing topics on failover
    // 为所有的topic之策ParitionModificationsListener
    controllerContext.allTopics.foreach(topic => registerPartitionModificationsListener(topic))
    info("Broker %d is ready to serve as the new controller with epoch %d".format(config.brokerId, epoch))

    // 处理重新分配的分区
    maybeTriggerPartitionReassignment()
    topicDeletionManager.tryTopicDeletion()
    val pendingPreferredReplicaElections = fetchPendingPreferredReplicaElections()
    onPreferredReplicaElection(pendingPreferredReplicaElections)
    info("starting the controller scheduler")
    kafkaScheduler.startup()

    //根据配置决定开启分区的自动均衡功能
    if (config.autoLeaderRebalanceEnable) {
      scheduleAutoLeaderRebalanceTask(delay = 5, unit = TimeUnit.SECONDS)
    }
  }

  private def scheduleAutoLeaderRebalanceTask(delay: Long, unit: TimeUnit): Unit = {
    // 启动auto-leader-rebalance-task定时任务
    kafkaScheduler.schedule("auto-leader-rebalance-task", () => eventManager.put(AutoPreferredReplicaLeaderElection),
      delay = delay, unit = unit)
  }

  /**
   * This callback is invoked by the zookeeper leader elector when the current broker resigns as the controller. This is
   * required to clean up internal controller data structures
   */
  // LeaderChangeListener 监听到/controller中的数据被删除或者改变时，
  // 旧的leader需要调用onResingAsLeader回调进行一些清理工作
  def onControllerResignation() {
    debug("Controller resigning, broker id %d".format(config.brokerId))
    // de-register listeners
    // 取消IsrChangeNotificationListener
    deregisterIsrChangeNotificationListener()
    deregisterPartitionReassignmentListener()
    deregisterPreferredReplicaElectionListener()

    // reset topic deletion manager
    topicDeletionManager.reset()

    // shutdown leader rebalance scheduler
    kafkaScheduler.shutdown()
    offlinePartitionCount = 0
    preferredReplicaImbalanceCount = 0

    // de-register partition ISR listener for on-going partition reassignment task
    // 取消所有 的ReassignmentIsrChangeListeners
    deregisterPartitionReassignmentIsrChangeListeners()

    // shutdown partition state machine
    // 关闭PartitionStateMachine
    partitionStateMachine.shutdown()
    deregisterTopicChangeListener()
    partitionModificationsListeners.keys.foreach(deregisterPartitionModificationsListener)
    deregisterTopicDeletionListener()
    // shutdown replica state machine
    replicaStateMachine.shutdown()
    deregisterBrokerChangeListener()

    resetControllerContext()

    info("Broker %d resigned as the controller".format(config.brokerId))
  }

  /**
   * Returns true if this broker is the current controller.
   */
  def isActive: Boolean = activeControllerId == config.brokerId

  /**
   * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
   * brokers as input. It does the following -
   * 1. Sends update metadata request to all live and shutting down brokers
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
   * 3. It checks whether there are reassigned replicas assigned to any newly started brokers.  If
   *    so, it performs the reassignment logic for each topic/partition.
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
   * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
   *    partitions currently new or offline (rather than every partition this controller is aware of)
   * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
   *    every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
   */
  //处理新增broker
  def onBrokerStartup(newBrokers: Seq[Int]) {
    info("New broker startup callback for %s".format(newBrokers.mkString(",")))
    val newBrokersSet = newBrokers.toSet
    // send update metadata request to all live and shutting down brokers. Old brokers will get to know of the new
    // broker via this update.
    // In cases of controlled shutdown leaders will not be elected when a new broker comes up. So at least in the
    // common controlled shutdown case, the metadata will reach the new brokers faster

    // 向集群中所有broker发送UpdateMetaRequest 发送所有
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)

    // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is
    // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions
    // 将新增broker上的副本转换为OnlineReplica，会发送LeaderAndIsrRequest和UpdateMetadataRequest
    val allReplicasOnNewBrokers = controllerContext.replicasOnBrokers(newBrokersSet)
    replicaStateMachine.handleStateChanges(allReplicasOnNewBrokers, OnlineReplica)

    // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
    // to see if these brokers can become leaders for some/all of those
    // 将NewPartition和OfflinePartiton状态的分区转换成OnlineParition
    partitionStateMachine.triggerOnlinePartitionStateChange()

    // check if reassignment of some partitions need to be restarted
    // 检测进行副本的重新分配
    val partitionsWithReplicasOnNewBrokers = controllerContext.partitionsBeingReassigned.filter {
      case (_, reassignmentContext) => reassignmentContext.newReplicas.exists(newBrokersSet.contains(_))
    }
    partitionsWithReplicasOnNewBrokers.foreach(p => onPartitionReassignment(p._1, p._2))
    // check if topic deletion needs to be resumed. If at least one replica that belongs to the topic being deleted exists
    // on the newly restarted brokers, there is a chance that topic deletion can resume
    // 如果新增Broker上有待删除的topic副本，则唤醒DeleteTopicsThread线程进行删除
    val replicasForTopicsToBeDeleted = allReplicasOnNewBrokers.filter(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))
    if(replicasForTopicsToBeDeleted.nonEmpty) {
      info(("Some replicas %s for topics scheduled for deletion %s are on the newly restarted brokers %s. " +
        "Signaling restart of topic deletion for these topics").format(replicasForTopicsToBeDeleted.mkString(","),
        topicDeletionManager.topicsToBeDeleted.mkString(","), newBrokers.mkString(",")))
      topicDeletionManager.resumeDeletionForTopics(replicasForTopicsToBeDeleted.map(_.topic))
    }
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
   * as input. It does the following -
   * 1. Mark partitions with dead leaders as offline
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
   * 3. Invokes the OfflineReplica state change on the input list of newly started brokers
   * 4. If no partitions are effected then send UpdateMetadataRequest to live or shutting down brokers
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point.  This is because
   * the partition state machine will refresh our cache for us when performing leader election for all new/offline
   * partitions coming online.
   */
  //处理故障broker
  def onBrokerFailure(deadBrokers: Seq[Int]) {

    info("Broker failure callback for %s".format(deadBrokers.mkString(",")))
    //将正在正常关闭的broker从deadBrokers列表中删除
    val deadBrokersThatWereShuttingDown =
      deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id))
    info("Removed %s from list of shutting down brokers.".format(deadBrokersThatWereShuttingDown))
    val deadBrokersSet = deadBrokers.toSet

    // trigger OfflinePartition state for all partitions whose current leader is one amongst the dead brokers
    // 过滤得到Leader副本在故障broker上的分区，将状态转换为OfflinePartition
    val partitionsWithoutLeader = controllerContext.partitionLeadershipInfo.filter(partitionAndLeader =>
      deadBrokersSet.contains(partitionAndLeader._2.leaderAndIsr.leader) &&
        !topicDeletionManager.isTopicQueuedUpForDeletion(partitionAndLeader._1.topic)).keySet
    partitionStateMachine.handleStateChanges(partitionsWithoutLeader, OfflinePartition)

    // trigger OnlinePartition state changes for offline or new partitions
    // 将offlineParition状态的分区转换为OnlineParition状态
    partitionStateMachine.triggerOnlinePartitionStateChange()

    // filter out the replicas that belong to topics that are being deleted
    // 过滤得到故障Broker上的副本，将这些副本转换为OfflineReplica状态
    var allReplicasOnDeadBrokers = controllerContext.replicasOnBrokers(deadBrokersSet)
    val activeReplicasOnDeadBrokers = allReplicasOnDeadBrokers.filterNot(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))
    // handle dead replicas
    replicaStateMachine.handleStateChanges(activeReplicasOnDeadBrokers, OfflineReplica)

    // check if topic deletion state for the dead replicas needs to be updated
    // 检查故障broker上面是否有待删除Topic的副本，如果存在，则将ReplicaDeletionTobeDeleted状态标记为不可删除
    val replicasForTopicsToBeDeleted = allReplicasOnDeadBrokers.filter(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))
    if(replicasForTopicsToBeDeleted.nonEmpty) {
      // it is required to mark the respective replicas in TopicDeletionFailed state since the replica cannot be
      // deleted when the broker is down. This will prevent the replica from being in TopicDeletionStarted state indefinitely
      // since topic deletion cannot be retried until at least one replica is in TopicDeletionStarted state
      topicDeletionManager.failReplicaDeletion(replicasForTopicsToBeDeleted)
    }

    // If broker failure did not require leader re-election, inform brokers of failed broker
    // Note that during leader re-election, brokers update their metadata
    //发送UpdateMetadataRequest更新所有Broker信息
    if (partitionsWithoutLeader.isEmpty) {
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)
    }
  }

  /**
   * This callback is invoked by the partition state machine's topic change listener with the list of new topics
   * and partitions as input. It does the following -
   * 1. Registers partition change listener. This is not required until KAFKA-347
   * 2. Invokes the new partition callback
   * 3. Send metadata request with the new topic to all brokers so they allow requests for that topic to be served
   */
  //完成新增topic的分区状态已经副本状态转换
  def onNewTopicCreation(topics: Set[String], newPartitions: Set[TopicAndPartition]) {
    info("New topic creation callback for %s".format(newPartitions.mkString(",")))
    // subscribe to partition changes
    //为每一个新增的topic都注册一个PartitionModificationsListener
    topics.foreach(topic => registerPartitionModificationsListener(topic))
    onNewPartitionCreation(newPartitions)
  }

  /**
   * This callback is invoked by the topic change callback with the list of failed brokers as input.
   * It does the following -
   * 1. Move the newly created partitions to the NewPartition state
   * 2. Move the newly created partitions from NewPartition->OnlinePartition state
   */
  def onNewPartitionCreation(newPartitions: Set[TopicAndPartition]) {
    info("New partition creation callback for %s".format(newPartitions.mkString(",")))
    //将所有指定的分区状态转换为NewPartition
    partitionStateMachine.handleStateChanges(newPartitions, NewPartition)
    //将指定分区的所有副本的状态转换为NewReplica
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), NewReplica)
    //将所有指定的新增分区转换为OnlinePartion状态
    partitionStateMachine.handleStateChanges(newPartitions, OnlinePartition, offlinePartitionSelector)
    //将既定分区的副本都转换为OnlineReplica状态
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), OnlineReplica)
  }

  /**
   * This callback is invoked by the reassigned partitions listener. When an admin command initiates a partition
   * reassignment, it creates the /admin/reassign_partitions path that triggers the zookeeper listener.
   * Reassigning replicas for a partition goes through a few steps listed in the code.
   * RAR = Reassigned replicas
   * OAR = Original list of replicas for partition
   * AR = current assigned replicas
   *
   * 1. Update AR in ZK with OAR + RAR.
   * 2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR). We do this by forcing an update
   *    of the leader epoch in zookeeper.
   * 3. Start new replicas RAR - OAR by moving replicas in RAR - OAR to NewReplica state.
   * 4. Wait until all replicas in RAR are in sync with the leader.
   * 5  Move all replicas in RAR to OnlineReplica state.
   * 6. Set AR to RAR in memory.
   * 7. If the leader is not in RAR, elect a new leader from RAR. If new leader needs to be elected from RAR, a LeaderAndIsr
   *    will be sent. If not, then leader epoch will be incremented in zookeeper and a LeaderAndIsr request will be sent.
   *    In any case, the LeaderAndIsr request will have AR = RAR. This will prevent the leader from adding any replica in
   *    RAR - OAR back in the isr.
   * 8. Move all replicas in OAR - RAR to OfflineReplica state. As part of OfflineReplica state change, we shrink the
   *    isr to remove OAR - RAR in zookeeper and send a LeaderAndIsr ONLY to the Leader to notify it of the shrunk isr.
   *    After that, we send a StopReplica (delete = false) to the replicas in OAR - RAR.
   * 9. Move all replicas in OAR - RAR to NonExistentReplica state. This will send a StopReplica (delete = true) to
   *    the replicas in OAR - RAR to physically delete the replicas on disk.
   * 10. Update AR in ZK with RAR.
   * 11. Update the /admin/reassign_partitions path in ZK to remove this partition.
   * 12. After electing leader, the replicas and isr information changes. So resend the update metadata request to every broker.
   *
   * For example, if OAR = {1, 2, 3} and RAR = {4,5,6}, the values in the assigned replica (AR) and leader/isr path in ZK
   * may go through the following transition.
   * AR                 leader/isr
   * {1,2,3}            1/{1,2,3}           (initial state)
   * {1,2,3,4,5,6}      1/{1,2,3}           (step 2)
   * {1,2,3,4,5,6}      1/{1,2,3,4,5,6}     (step 4)
   * {1,2,3,4,5,6}      4/{1,2,3,4,5,6}     (step 7)
   * {1,2,3,4,5,6}      4/{4,5,6}           (step 8)
   * {4,5,6}            4/{4,5,6}           (step 10)
   *
   * Note that we have to update AR in ZK with RAR last since it's the only place where we store OAR persistently.
   * This way, if the controller crashes before that step, we can still recover.
   */
  // 副本重新分配
  def onPartitionReassignment(topicAndPartition: TopicAndPartition, reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    if (!areReplicasInIsr(topicAndPartition.topic, topicAndPartition.partition, reassignedReplicas)) {
      info("New replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
        "reassigned not yet caught up with the leader")
      val newReplicasNotInOldReplicaList = reassignedReplicas.toSet -- controllerContext.partitionReplicaAssignment(topicAndPartition).toSet
      val newAndOldReplicas = (reassignedPartitionContext.newReplicas ++ controllerContext.partitionReplicaAssignment(topicAndPartition)).toSet

      //1. Update AR in ZK with OAR + RAR.
      // 将partition在contextcontroller和zk中的ar集合更新成 新ar+旧ar
      updateAssignedReplicasForPartition(topicAndPartition, newAndOldReplicas.toSeq)

      //2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR).
      // 通过发送LeaderAndisrreequest 递增leader_epoch
      updateLeaderEpochAndSendRequest(topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition),
        newAndOldReplicas.toSeq)

      //3. replicas in RAR - OAR -> NewReplica
      // 将新ar+旧ar中副本更新成newreplica状态
      startNewReplicasForReassignedPartition(topicAndPartition, reassignedPartitionContext, newReplicasNotInOldReplicaList)
      info("Waiting for new replicas %s for partition %s being ".format(reassignedReplicas.mkString(","), topicAndPartition) +
        "reassigned to catch up with the leader")
    } else {
      //4. Wait until all replicas in RAR are in sync with the leader.
      // 获取旧ar集合
      val oldReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).toSet -- reassignedReplicas.toSet

      //5. replicas in RAR -> OnlineReplica
      // 将新ar集合中的所有副本都转换成OnlieReplica状态
      reassignedReplicas.foreach { replica =>
        replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition,
          replica)), OnlineReplica)
      }
      //6. Set AR to RAR in memory.
      //7. Send LeaderAndIsr request with a potential new leader (if current leader not in RAR) and
      //   a new AR (using RAR) and same isr to every broker in RAR
      moveReassignedPartitionLeaderIfRequired(topicAndPartition, reassignedPartitionContext)
      //8. replicas in OAR - RAR -> Offline (force those replicas out of isr)
      //9. replicas in OAR - RAR -> NonExistentReplica (force those replicas to be deleted)
      // 将新 旧ar-新ar中的副本转换成OfflineReplica
      stopOldReplicasOfReassignedPartition(topicAndPartition, reassignedPartitionContext, oldReplicas)

      //10. Update AR in ZK with RAR.
      // 更新zk中的记录的ar信息
      updateAssignedReplicasForPartition(topicAndPartition, reassignedReplicas)

      //11. Update the /admin/reassign_partitions path in ZK to remove this partition.
      // 将parition相关信息从zk中删除
      removePartitionFromReassignedPartitions(topicAndPartition)
      info("Removed partition %s from the list of reassigned partitions in zookeeper".format(topicAndPartition))
      controllerContext.partitionsBeingReassigned.remove(topicAndPartition)

      //12. After electing leader, the replicas and isr information changes, so resend the update metadata request to every broker
      // 像所有可用的broker发一次updateMetadateRequest
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicAndPartition))
      // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed
      // 尝试取消相关的topic的不可删除标记。并唤醒DeleteTopicThread
      topicDeletionManager.resumeDeletionForTopics(Set(topicAndPartition.topic))
    }
  }

  private def watchIsrChangesForReassignedPartition(topic: String,
                                                    partition: Int,
                                                    reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val isrChangeListener = new PartitionReassignmentIsrChangeListener(this, eventManager, topic, partition,
      reassignedReplicas.toSet)
    reassignedPartitionContext.isrChangeListener = isrChangeListener
    // register listener on the leader and isr path to wait until they catch up with the current leader
    zkUtils.zkClient.subscribeDataChanges(getTopicPartitionLeaderAndIsrPath(topic, partition), isrChangeListener)
  }

  def initiateReassignReplicasForTopicPartition(topicAndPartition: TopicAndPartition,
                                        reassignedPartitionContext: ReassignedPartitionsContext) {
    val newReplicas = reassignedPartitionContext.newReplicas
    val topic = topicAndPartition.topic
    val partition = topicAndPartition.partition
    try {
      val assignedReplicasOpt = controllerContext.partitionReplicaAssignment.get(topicAndPartition)
      assignedReplicasOpt match {
        case Some(assignedReplicas) =>
          // 新旧副本分配方式完全一样
          if (assignedReplicas == newReplicas) {
            throw new KafkaException("Partition %s to be reassigned is already assigned to replicas".format(topicAndPartition) +
              " %s. Ignoring request for partition reassignment".format(newReplicas.mkString(",")))
          } else {
            info("Handling reassignment of partition %s to new replicas %s".format(topicAndPartition, newReplicas.mkString(",")))
            // first register ISR change listener
            // 为分区注册ReassignedPartitionsContext
            watchIsrChangesForReassignedPartition(topic, partition, reassignedPartitionContext)
            controllerContext.partitionsBeingReassigned.put(topicAndPartition, reassignedPartitionContext)
            // mark topic ineligible for deletion for the partitions being reassigned
            // 将topic标记为不可删除
            topicDeletionManager.markTopicIneligibleForDeletion(Set(topic))
            // 执行副本的重新分配
            onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
          }
        case None => throw new KafkaException("Attempt to reassign partition %s that doesn't exist"
          .format(topicAndPartition))
      }
    } catch {
      case e: Throwable => error("Error completing reassignment of partition %s".format(topicAndPartition), e)
      // remove the partition from the admin path to unblock the admin client
      removePartitionFromReassignedPartitions(topicAndPartition)
    }
  }

  def onPreferredReplicaElection(partitions: Set[TopicAndPartition], isTriggeredByAutoRebalance: Boolean = false) {
    info("Starting preferred replica leader election for partitions %s".format(partitions.mkString(",")))
    try {
      partitionStateMachine.handleStateChanges(partitions, OnlinePartition, preferredReplicaPartitionLeaderSelector)
    } catch {
      case e: Throwable => error("Error completing preferred replica leader election for partitions %s".format(partitions.mkString(",")), e)
    } finally {
      removePartitionsFromPreferredReplicaElection(partitions, isTriggeredByAutoRebalance)
    }
  }

  /**
   * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
   * is the controller. It merely registers the session expiration listener and starts the controller leader
   * elector
   */
  // Starup启动
  def startup() = {
    eventManager.put(Startup)
    eventManager.start()
  }

  /**
   * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
   * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
   * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
   */
  def shutdown() = {
    eventManager.close()
    onControllerResignation()
  }

  def sendRequest(brokerId: Int, apiKey: ApiKeys, request: AbstractRequest.Builder[_ <: AbstractRequest],
                  callback: AbstractResponse => Unit = null) = {
    controllerContext.controllerChannelManager.sendRequest(brokerId, apiKey, request, callback)
  }

  def incrementControllerEpoch() = {
    try {
      var newControllerEpoch = controllerContext.epoch + 1
      val (updateSucceeded, newVersion) = zkUtils.conditionalUpdatePersistentPathIfExists(
        ZkUtils.ControllerEpochPath, newControllerEpoch.toString, controllerContext.epochZkVersion)
      if(!updateSucceeded)
        throw new ControllerMovedException("Controller moved to another broker. Aborting controller startup procedure")
      else {
        controllerContext.epochZkVersion = newVersion
        controllerContext.epoch = newControllerEpoch
      }
    } catch {
      case _: ZkNoNodeException =>
        // if path doesn't exist, this is the first controller whose epoch should be 1
        // the following call can still fail if another controller gets elected between checking if the path exists and
        // trying to create the controller epoch path
        try {
          zkUtils.createPersistentPath(ZkUtils.ControllerEpochPath, KafkaController.InitialControllerEpoch.toString)
          controllerContext.epoch = KafkaController.InitialControllerEpoch
          controllerContext.epochZkVersion = KafkaController.InitialControllerEpochZkVersion
        } catch {
          case _: ZkNodeExistsException => throw new ControllerMovedException("Controller moved to another broker. " +
            "Aborting controller startup procedure")
          case oe: Throwable => error("Error while incrementing controller epoch", oe)
        }
      case oe: Throwable => error("Error while incrementing controller epoch", oe)

    }
    info("Controller %d incremented epoch to %d".format(config.brokerId, controllerContext.epoch))
  }

  private def registerSessionExpirationListener() = {
    zkUtils.zkClient.subscribeStateChanges(new SessionExpirationListener(this, eventManager))
  }

  private def registerControllerChangeListener() = {
    zkUtils.zkClient.subscribeDataChanges(ZkUtils.ControllerPath, new ControllerChangeListener(this, eventManager))
  }

  private def initializeControllerContext() {
    // update controller cache with delete topic information
    // 读取 /brokers/ids 初始化可用broker集合
    controllerContext.liveBrokers = zkUtils.getAllBrokersInCluster().toSet

    // 读取 /brokers/topics 初始化集群中全部的topic信息
    controllerContext.allTopics = zkUtils.getAllTopics().toSet

    // 读取 /brokers/topics/[topic_name]/paritons 初始化每个parition的ar集合信息
    controllerContext.partitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(controllerContext.allTopics.toSeq)
    controllerContext.partitionLeadershipInfo = new mutable.HashMap[TopicAndPartition, LeaderIsrAndControllerEpoch]
    controllerContext.shuttingDownBrokerIds = mutable.Set.empty[Int]

    // update the leader and isr cache for all existing partitions from Zookeeper
    // 读取 /brokers/topics/[topic_name]/partitions/[partitionId]/state 初始化每个paritition的leader isr集合信息
    updateLeaderAndIsrCache()

    // start the channel manager
    // 启动ControllerChannleManager
    startChannelManager()

    // 读取 /admin/reagging_partitions 初始化优先副本选举的partition
    initializePartitionReassignment()
    info("Currently active brokers in the cluster: %s".format(controllerContext.liveBrokerIds))
    info("Currently shutting brokers in the cluster: %s".format(controllerContext.shuttingDownBrokerIds))
    info("Current list of topics in the cluster: %s".format(controllerContext.allTopics))
  }

  private def fetchPendingPreferredReplicaElections(): Set[TopicAndPartition] = {
    val partitionsUndergoingPreferredReplicaElection = zkUtils.getPartitionsUndergoingPreferredReplicaElection()
    // check if they are already completed or topic was deleted
    val partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.filter { partition =>
      val replicasOpt = controllerContext.partitionReplicaAssignment.get(partition)
      val topicDeleted = replicasOpt.isEmpty
      val successful =
        if(!topicDeleted) controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader == replicasOpt.get.head else false
      successful || topicDeleted
    }
    val pendingPreferredReplicaElectionsIgnoringTopicDeletion = partitionsUndergoingPreferredReplicaElection -- partitionsThatCompletedPreferredReplicaElection
    val pendingPreferredReplicaElectionsSkippedFromTopicDeletion = pendingPreferredReplicaElectionsIgnoringTopicDeletion.filter(partition => topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic))
    val pendingPreferredReplicaElections = pendingPreferredReplicaElectionsIgnoringTopicDeletion -- pendingPreferredReplicaElectionsSkippedFromTopicDeletion
    info("Partitions undergoing preferred replica election: %s".format(partitionsUndergoingPreferredReplicaElection.mkString(",")))
    info("Partitions that completed preferred replica election: %s".format(partitionsThatCompletedPreferredReplicaElection.mkString(",")))
    info("Skipping preferred replica election for partitions due to topic deletion: %s".format(pendingPreferredReplicaElectionsSkippedFromTopicDeletion.mkString(",")))
    info("Resuming preferred replica election for partitions: %s".format(pendingPreferredReplicaElections.mkString(",")))
    pendingPreferredReplicaElections
  }

  private def resetControllerContext(): Unit = {
    if (controllerContext.controllerChannelManager != null) {
      controllerContext.controllerChannelManager.shutdown()
      controllerContext.controllerChannelManager = null
    }
    controllerContext.shuttingDownBrokerIds.clear()
    controllerContext.epoch = 0
    controllerContext.epochZkVersion = 0
    controllerContext.allTopics = Set.empty
    controllerContext.partitionReplicaAssignment.clear()
    controllerContext.partitionLeadershipInfo.clear()
    controllerContext.partitionsBeingReassigned.clear()
    controllerContext.liveBrokers = Set.empty
  }

  private def initializePartitionReassignment() {
    // read the partitions being reassigned from zookeeper path /admin/reassign_partitions
    val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned()
    // check if they are already completed or topic was deleted
    val reassignedPartitions = partitionsBeingReassigned.filter { partition =>
      val replicasOpt = controllerContext.partitionReplicaAssignment.get(partition._1)
      val topicDeleted = replicasOpt.isEmpty
      val successful = if (!topicDeleted) replicasOpt.get == partition._2.newReplicas else false
      topicDeleted || successful
    }.keys
    reassignedPartitions.foreach(p => removePartitionFromReassignedPartitions(p))
    var partitionsToReassign: mutable.Map[TopicAndPartition, ReassignedPartitionsContext] = new mutable.HashMap
    partitionsToReassign ++= partitionsBeingReassigned
    partitionsToReassign --= reassignedPartitions
    controllerContext.partitionsBeingReassigned ++= partitionsToReassign
    info("Partitions being reassigned: %s".format(partitionsBeingReassigned.toString()))
    info("Partitions already reassigned: %s".format(reassignedPartitions.toString()))
    info("Resuming reassignment of partitions: %s".format(partitionsToReassign.toString()))
  }

  private def fetchTopicDeletionsInProgress(): (Set[String], Set[String]) = {
    val topicsToBeDeleted = zkUtils.getChildrenParentMayNotExist(ZkUtils.DeleteTopicsPath).toSet
    val topicsWithReplicasOnDeadBrokers = controllerContext.partitionReplicaAssignment.filter { case (_, replicas) =>
      replicas.exists(r => !controllerContext.liveBrokerIds.contains(r)) }.keySet.map(_.topic)
    val topicsForWhichPartitionReassignmentIsInProgress = controllerContext.partitionsBeingReassigned.keySet.map(_.topic)
    val topicsIneligibleForDeletion = topicsWithReplicasOnDeadBrokers | topicsForWhichPartitionReassignmentIsInProgress
    info("List of topics to be deleted: %s".format(topicsToBeDeleted.mkString(",")))
    info("List of topics ineligible for deletion: %s".format(topicsIneligibleForDeletion.mkString(",")))
    (topicsToBeDeleted, topicsIneligibleForDeletion)
  }

  private def maybeTriggerPartitionReassignment() {
    controllerContext.partitionsBeingReassigned.foreach { topicPartitionToReassign =>
      initiateReassignReplicasForTopicPartition(topicPartitionToReassign._1, topicPartitionToReassign._2)
    }
  }

  private def startChannelManager() {
    controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config, time, metrics, threadNamePrefix)
    controllerContext.controllerChannelManager.startup()
  }

  def updateLeaderAndIsrCache(topicAndPartitions: Set[TopicAndPartition] = controllerContext.partitionReplicaAssignment.keySet) {
    val leaderAndIsrInfo = zkUtils.getPartitionLeaderAndIsrForTopics(topicAndPartitions)
    for ((topicPartition, leaderIsrAndControllerEpoch) <- leaderAndIsrInfo)
      controllerContext.partitionLeadershipInfo.put(topicPartition, leaderIsrAndControllerEpoch)
  }

  private def areReplicasInIsr(topic: String, partition: Int, replicas: Seq[Int]): Boolean = {
    zkUtils.getLeaderAndIsrForPartition(topic, partition).map { leaderAndIsr =>
      replicas.forall(leaderAndIsr.isr.contains)
    }.getOrElse(false)
  }

  private def moveReassignedPartitionLeaderIfRequired(topicAndPartition: TopicAndPartition,
                                                      reassignedPartitionContext: ReassignedPartitionsContext) {
    val reassignedReplicas = reassignedPartitionContext.newReplicas
    val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
    // change the assigned replica list to just the reassigned replicas in the cache so it gets sent out on the LeaderAndIsr
    // request to the current or new leader. This will prevent it from adding the old replicas to the ISR
    val oldAndNewReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    controllerContext.partitionReplicaAssignment.put(topicAndPartition, reassignedReplicas)
    if(!reassignedPartitionContext.newReplicas.contains(currentLeader)) {
      info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
        "is not in the new list of replicas %s. Re-electing leader".format(reassignedReplicas.mkString(",")))
      // move the leader to one of the alive and caught up new replicas
      partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
    } else {
      // check if the leader is alive or not
      if (controllerContext.liveBrokerIds.contains(currentLeader)) {
        info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
          "is already in the new list of replicas %s and is alive".format(reassignedReplicas.mkString(",")))
        // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
        updateLeaderEpochAndSendRequest(topicAndPartition, oldAndNewReplicas, reassignedReplicas)
      } else {
        info("Leader %s for partition %s being reassigned, ".format(currentLeader, topicAndPartition) +
          "is already in the new list of replicas %s but is dead".format(reassignedReplicas.mkString(",")))
        partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector)
      }
    }
  }

  private def stopOldReplicasOfReassignedPartition(topicAndPartition: TopicAndPartition,
                                                   reassignedPartitionContext: ReassignedPartitionsContext,
                                                   oldReplicas: Set[Int]) {
    val topic = topicAndPartition.topic
    val partition = topicAndPartition.partition
    // first move the replica to offline state (the controller removes it from the ISR)
    val replicasToBeDeleted = oldReplicas.map(r => PartitionAndReplica(topic, partition, r))
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, OfflineReplica)
    // send stop replica command to the old replicas
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionStarted)
    // TODO: Eventually partition reassignment could use a callback that does retries if deletion failed
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionSuccessful)
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, NonExistentReplica)
  }

  private def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                                 replicas: Seq[Int]) {
    val partitionsAndReplicasForThisTopic = controllerContext.partitionReplicaAssignment.filter(_._1.topic.equals(topicAndPartition.topic))
    partitionsAndReplicasForThisTopic.put(topicAndPartition, replicas)
    updateAssignedReplicasForPartition(topicAndPartition, partitionsAndReplicasForThisTopic)
    info("Updated assigned replicas for partition %s being reassigned to %s ".format(topicAndPartition, replicas.mkString(",")))
    // update the assigned replica list after a successful zookeeper write
    controllerContext.partitionReplicaAssignment.put(topicAndPartition, replicas)
  }

  private def startNewReplicasForReassignedPartition(topicAndPartition: TopicAndPartition,
                                                     reassignedPartitionContext: ReassignedPartitionsContext,
                                                     newReplicas: Set[Int]) {
    // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
    // replicas list
    newReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, replica)), NewReplica)
    }
  }

  private def updateLeaderEpochAndSendRequest(topicAndPartition: TopicAndPartition, replicasToReceiveRequest: Seq[Int], newAssignedReplicas: Seq[Int]) {
    updateLeaderEpoch(topicAndPartition.topic, topicAndPartition.partition) match {
      case Some(updatedLeaderIsrAndControllerEpoch) =>
        try {
          brokerRequestBatch.newBatch()
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasToReceiveRequest, topicAndPartition.topic,
            topicAndPartition.partition, updatedLeaderIsrAndControllerEpoch, newAssignedReplicas)
          brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
        } catch {
          case e: IllegalStateException =>
            handleIllegalState(e)
        }
        stateChangeLogger.trace(("Controller %d epoch %d sent LeaderAndIsr request %s with new assigned replica list %s " +
          "to leader %d for partition being reassigned %s").format(config.brokerId, controllerContext.epoch, updatedLeaderIsrAndControllerEpoch,
          newAssignedReplicas.mkString(","), updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader, topicAndPartition))
      case None => // fail the reassignment
        stateChangeLogger.error(("Controller %d epoch %d failed to send LeaderAndIsr request with new assigned replica list %s " +
          "to leader for partition being reassigned %s").format(config.brokerId, controllerContext.epoch,
          newAssignedReplicas.mkString(","), topicAndPartition))
    }
  }

  private def registerBrokerChangeListener() = {
    zkUtils.zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
  }

  private def deregisterBrokerChangeListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
  }

  private def registerTopicChangeListener() = {
    zkUtils.zkClient.subscribeChildChanges(BrokerTopicsPath, topicChangeListener)
  }

  private def deregisterTopicChangeListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(BrokerTopicsPath, topicChangeListener)
  }

  def registerPartitionModificationsListener(topic: String) = {
    partitionModificationsListeners.put(topic, new PartitionModificationsListener(this, eventManager, topic))
    zkUtils.zkClient.subscribeDataChanges(getTopicPath(topic), partitionModificationsListeners(topic))
  }

  def deregisterPartitionModificationsListener(topic: String) = {
    zkUtils.zkClient.unsubscribeDataChanges(getTopicPath(topic), partitionModificationsListeners(topic))
    partitionModificationsListeners.remove(topic)
  }

  private def registerTopicDeletionListener() = {
    zkUtils.zkClient.subscribeChildChanges(DeleteTopicsPath, topicDeletionListener)
  }

  private def deregisterTopicDeletionListener() = {
    zkUtils.zkClient.unsubscribeChildChanges(DeleteTopicsPath, topicDeletionListener)
  }

  private def registerPartitionReassignmentListener() = {
    zkUtils.zkClient.subscribeDataChanges(ZkUtils.ReassignPartitionsPath, partitionReassignmentListener)
  }

  private def deregisterPartitionReassignmentListener() = {
    zkUtils.zkClient.unsubscribeDataChanges(ZkUtils.ReassignPartitionsPath, partitionReassignmentListener)
  }

  private def registerIsrChangeNotificationListener() = {
    debug("Registering IsrChangeNotificationListener")
    zkUtils.zkClient.subscribeChildChanges(ZkUtils.IsrChangeNotificationPath, isrChangeNotificationListener)
  }

  private def deregisterIsrChangeNotificationListener() = {
    debug("De-registering IsrChangeNotificationListener")
    zkUtils.zkClient.unsubscribeChildChanges(ZkUtils.IsrChangeNotificationPath, isrChangeNotificationListener)
  }

  private def registerPreferredReplicaElectionListener() {
    zkUtils.zkClient.subscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, preferredReplicaElectionListener)
  }

  private def deregisterPreferredReplicaElectionListener() {
    zkUtils.zkClient.unsubscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, preferredReplicaElectionListener)
  }

  private def deregisterPartitionReassignmentIsrChangeListeners() {
    controllerContext.partitionsBeingReassigned.foreach {
      case (topicAndPartition, reassignedPartitionsContext) =>
        val zkPartitionPath = getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition)
        zkUtils.zkClient.unsubscribeDataChanges(zkPartitionPath, reassignedPartitionsContext.isrChangeListener)
    }
  }

  private def readControllerEpochFromZookeeper() {
    // initialize the controller epoch and zk version by reading from zookeeper
    if(controllerContext.zkUtils.pathExists(ZkUtils.ControllerEpochPath)) {
      val epochData = controllerContext.zkUtils.readData(ZkUtils.ControllerEpochPath)
      controllerContext.epoch = epochData._1.toInt
      controllerContext.epochZkVersion = epochData._2.getVersion
      info("Initialized controller epoch to %d and zk version %d".format(controllerContext.epoch, controllerContext.epochZkVersion))
    }
  }

  def removePartitionFromReassignedPartitions(topicAndPartition: TopicAndPartition) {
    if(controllerContext.partitionsBeingReassigned.get(topicAndPartition).isDefined) {
      // stop watching the ISR changes for this partition
      zkUtils.zkClient.unsubscribeDataChanges(getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
        controllerContext.partitionsBeingReassigned(topicAndPartition).isrChangeListener)
    }
    // read the current list of reassigned partitions from zookeeper
    val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned()
    // remove this partition from that list
    val updatedPartitionsBeingReassigned = partitionsBeingReassigned - topicAndPartition
    // write the new list to zookeeper
    zkUtils.updatePartitionReassignmentData(updatedPartitionsBeingReassigned.mapValues(_.newReplicas))
    // update the cache. NO-OP if the partition's reassignment was never started
    controllerContext.partitionsBeingReassigned.remove(topicAndPartition)
  }

  def updateAssignedReplicasForPartition(topicAndPartition: TopicAndPartition,
                                         newReplicaAssignmentForTopic: Map[TopicAndPartition, Seq[Int]]) {
    try {
      val zkPath = getTopicPath(topicAndPartition.topic)
      val jsonPartitionMap = zkUtils.replicaAssignmentZkData(newReplicaAssignmentForTopic.map(e => e._1.partition.toString -> e._2))
      zkUtils.updatePersistentPath(zkPath, jsonPartitionMap)
      debug("Updated path %s with %s for replica assignment".format(zkPath, jsonPartitionMap))
    } catch {
      case _: ZkNoNodeException => throw new IllegalStateException("Topic %s doesn't exist".format(topicAndPartition.topic))
      case e2: Throwable => throw new KafkaException(e2.toString)
    }
  }

  def removePartitionsFromPreferredReplicaElection(partitionsToBeRemoved: Set[TopicAndPartition],
                                                   isTriggeredByAutoRebalance : Boolean) {
    for(partition <- partitionsToBeRemoved) {
      // check the status
      val currentLeader = controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader
      val preferredReplica = controllerContext.partitionReplicaAssignment(partition).head
      if(currentLeader == preferredReplica) {
        info("Partition %s completed preferred replica leader election. New leader is %d".format(partition, preferredReplica))
      } else {
        warn("Partition %s failed to complete preferred replica leader election. Leader is %d".format(partition, currentLeader))
      }
    }
    if (!isTriggeredByAutoRebalance)
      zkUtils.deletePath(ZkUtils.PreferredReplicaLeaderElectionPath)
  }

  /**
   * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
   * metadata requests
   *
   * @param brokers The brokers that the update metadata request should be sent to
   */
  def sendUpdateMetadataRequest(brokers: Seq[Int], partitions: Set[TopicAndPartition] = Set.empty[TopicAndPartition]) {
    try {
      brokerRequestBatch.newBatch()
      brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions)
      brokerRequestBatch.sendRequestsToBrokers(epoch)
    } catch {
      case e: IllegalStateException =>
        handleIllegalState(e)
    }
  }

  /**
   * Removes a given partition replica from the ISR; if it is not the current
   * leader and there are sufficient remaining replicas in ISR.
   *
   * @param topic topic
   * @param partition partition
   * @param replicaId replica Id
   * @return the new leaderAndIsr (with the replica removed if it was present),
   *         or None if leaderAndIsr is empty.
   */
  def removeReplicaFromIsr(topic: String, partition: Int, replicaId: Int): Option[LeaderIsrAndControllerEpoch] = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    debug("Removing replica %d from ISR %s for partition %s.".format(replicaId,
      controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.isr.mkString(","), topicAndPartition))
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      val leaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
      zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
        case Some(leaderIsrAndEpoch) => // increment the leader epoch even if the ISR changes
          val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndEpoch.controllerEpoch
          if(controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
              "means the current controller with epoch %d went through a soft failure and another ".format(epoch) +
              "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch))
          if (leaderAndIsr.isr.contains(replicaId)) {
            // if the replica to be removed from the ISR is also the leader, set the new leader value to -1
            val newLeader = if (replicaId == leaderAndIsr.leader) LeaderAndIsr.NoLeader else leaderAndIsr.leader
            var newIsr = leaderAndIsr.isr.filter(b => b != replicaId)

            // if the replica to be removed from the ISR is the last surviving member of the ISR and unclean leader election
            // is disallowed for the corresponding topic, then we must preserve the ISR membership so that the replica can
            // eventually be restored as the leader.
            if (newIsr.isEmpty && !LogConfig.fromProps(config.originals, AdminUtils.fetchEntityConfig(zkUtils,
              ConfigType.Topic, topicAndPartition.topic)).uncleanLeaderElectionEnable) {
              info("Retaining last ISR %d of partition %s since unclean leader election is disabled".format(replicaId, topicAndPartition))
              newIsr = leaderAndIsr.isr
            }

            val newLeaderAndIsr = leaderAndIsr.newLeaderAndIsr(newLeader, newIsr)
            // update the new leadership decision in zookeeper or retry
            val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partition,
              newLeaderAndIsr, epoch, leaderAndIsr.zkVersion)

            val leaderWithNewVersion = newLeaderAndIsr.withZkVersion(newVersion)
            finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(leaderWithNewVersion, epoch))
            controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get)
            if (updateSucceeded) {
              info(s"New leader and ISR for partition $topicAndPartition is $leaderWithNewVersion")
            }
            updateSucceeded
          } else {
            warn(s"Cannot remove replica $replicaId from ISR of partition $topicAndPartition since it is not in the ISR." +
              s" Leader = ${leaderAndIsr.leader} ; ISR = ${leaderAndIsr.isr}")
            finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(leaderAndIsr, epoch))
            controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get)
            true
          }
        case None =>
          warn("Cannot remove replica %d from ISR of %s - leaderAndIsr is empty.".format(replicaId, topicAndPartition))
          true
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  /**
   * Does not change leader or isr, but just increments the leader epoch
   *
   * @param topic topic
   * @param partition partition
   * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
   */
  private def updateLeaderEpoch(topic: String, partition: Int): Option[LeaderIsrAndControllerEpoch] = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    debug("Updating leader epoch for partition %s.".format(topicAndPartition))
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      val leaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
      zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
        case Some(leaderIsrAndEpoch) =>
          val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndEpoch.controllerEpoch
          if(controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
              "means the current controller with epoch %d went through a soft failure and another ".format(epoch) +
              "controller was elected with epoch %d. Aborting state change by this controller".format(controllerEpoch))
          // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded
          // assigned replica list
          val newLeaderAndIsr = leaderAndIsr.newEpochAndZkVersion
          // update the new leadership decision in zookeeper or retry
          val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic,
            partition, newLeaderAndIsr, epoch, leaderAndIsr.zkVersion)

          val leaderWithNewVersion = newLeaderAndIsr.withZkVersion(newVersion)
          finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(leaderWithNewVersion, epoch))
          if (updateSucceeded) {
            info(s"Updated leader epoch for partition $topicAndPartition to ${leaderWithNewVersion.leaderEpoch}")
          }
          updateSucceeded
        case None =>
          throw new IllegalStateException(s"Cannot update leader epoch for partition $topicAndPartition as " +
            "leaderAndIsr path is empty. This could mean we somehow tried to reassign a partition that doesn't exist")
          true
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  private def checkAndTriggerAutoLeaderRebalance(): Unit = {
    trace("Checking need to trigger auto leader balancing")
    // 获取所有可用的broker副本
    val preferredReplicasForTopicsByBrokers: Map[Int, Map[TopicAndPartition, Seq[Int]]] =
      controllerContext.partitionReplicaAssignment.filterNot { case (tp, _) =>
        topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic)
      }.groupBy { case (_, assignedReplicas) => assignedReplicas.head }
    debug(s"Preferred replicas by broker $preferredReplicasForTopicsByBrokers")

    // for each broker, check if a preferred replica election needs to be triggered
    // 计算每个broker的 imbalance比例
    preferredReplicasForTopicsByBrokers.foreach { case (leaderBroker, topicAndPartitionsForBroker) =>
      val topicsNotInPreferredReplica = topicAndPartitionsForBroker.filter { case (topicPartition, _) =>
        val leadershipInfo = controllerContext.partitionLeadershipInfo.get(topicPartition)
        leadershipInfo.map(_.leaderAndIsr.leader != leaderBroker).getOrElse(false)
      }
      debug(s"Topics not in preferred replica $topicsNotInPreferredReplica")

      val imbalanceRatio = topicsNotInPreferredReplica.size.toDouble / topicAndPartitionsForBroker.size
      trace(s"Leader imbalance ratio for broker $leaderBroker is $imbalanceRatio")

      // check ratio and if greater than desired ratio, trigger a rebalance for the topic partitions
      // that need to be on this broker
      if (imbalanceRatio > (config.leaderImbalancePerBrokerPercentage.toDouble / 100)) {
        topicsNotInPreferredReplica.keys.foreach { topicPartition =>
          // do this check only if the broker is live and there are no partitions being reassigned currently
          // and preferred replica election is not in progress
          if (controllerContext.liveBrokerIds.contains(leaderBroker) &&
            controllerContext.partitionsBeingReassigned.isEmpty &&
            !topicDeletionManager.isTopicQueuedUpForDeletion(topicPartition.topic) &&
            controllerContext.allTopics.contains(topicPartition.topic)) {
            // 触发优选副本选举
            onPreferredReplicaElection(Set(topicPartition), isTriggeredByAutoRebalance = true)
          }
        }
      }
    }
  }

  def getControllerID(): Int = {
    controllerContext.zkUtils.readDataMaybeNull(ZkUtils.ControllerPath)._1 match {
      case Some(controller) => KafkaController.parseControllerId(controller)
      case None => -1
    }
  }

  case class BrokerChange(currentBrokerList: Seq[String]) extends ControllerEvent {

    def state = ControllerState.BrokerChange

    override def process(): Unit = {
      if (!isActive) return
      // Read the current broker list from ZK again instead of using currentBrokerList to increase
      // the odds of processing recent broker changes in a single ControllerEvent (KAFKA-5502).

      //获得zk中的broker列表
      val curBrokers = zkUtils.getAllBrokersInCluster().toSet
      val curBrokerIds = curBrokers.map(_.id)
      val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
      val newBrokerIds = curBrokerIds -- liveOrShuttingDownBrokerIds
      val deadBrokerIds = liveOrShuttingDownBrokerIds -- curBrokerIds
      //过滤得到新增的broker列表
      val newBrokers = curBrokers.filter(broker => newBrokerIds(broker.id))
      controllerContext.liveBrokers = curBrokers
      val newBrokerIdsSorted = newBrokerIds.toSeq.sorted
      //过滤，得到故障的broker列表
      val deadBrokerIdsSorted = deadBrokerIds.toSeq.sorted
      val liveBrokerIdsSorted = curBrokerIds.toSeq.sorted
      info("Newly added brokers: %s, deleted brokers: %s, all live brokers: %s"
        .format(newBrokerIdsSorted.mkString(","), deadBrokerIdsSorted.mkString(","), liveBrokerIdsSorted.mkString(",")))
      //处理Controller与新增的broker的网络连接
      newBrokers.foreach(controllerContext.controllerChannelManager.addBroker)
      //关闭Controller与故障broker的网络连接
      deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker)
      if (newBrokerIds.nonEmpty)
        //处理新增broker
        onBrokerStartup(newBrokerIdsSorted)
      if (deadBrokerIds.nonEmpty)
        //处理下线broker
        onBrokerFailure(deadBrokerIdsSorted)
    }
  }

  case class TopicChange(topics: Set[String]) extends ControllerEvent {

    def state = ControllerState.TopicChange

    override def process(): Unit = {
      if (!isActive) return
      //得到新的topic
      val newTopics = topics -- controllerContext.allTopics
      //得到删除的topic
      val deletedTopics = controllerContext.allTopics -- topics

      //更新controllerContext的topic集合
      controllerContext.allTopics = topics
      //获取新增topic的分区信息以及ar集合信息
      val addedPartitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(newTopics.toSeq)、
      //更新ControllerContext中的ar集合记录
      controllerContext.partitionReplicaAssignment = controllerContext.partitionReplicaAssignment.filter(p =>
        !deletedTopics.contains(p._1.topic))
      controllerContext.partitionReplicaAssignment.++=(addedPartitionReplicaAssignment)
      info("New topics: [%s], deleted topics: [%s], new partition replica assignment [%s]".format(newTopics,
        deletedTopics, addedPartitionReplicaAssignment))
      if (newTopics.nonEmpty)
        //处理新增的topic
        onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet)
    }
  }

  case class PartitionModifications(topic: String) extends ControllerEvent {

    def state = ControllerState.TopicChange

    override def process(): Unit = {
      if (!isActive) return
      //从zk /brokers/topics/[topic_name]路径下加载每个Partiton的ar集合
      val partitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(List(topic))
      //过滤出新增分区记录
      val partitionsToBeAdded = partitionReplicaAssignment.filter(p =>
        !controllerContext.partitionReplicaAssignment.contains(p._1))
      //正在删除
      if(topicDeletionManager.isTopicQueuedUpForDeletion(topic))
        error("Skipping adding partitions %s for topic %s since it is currently being deleted"
          .format(partitionsToBeAdded.map(_._1.partition).mkString(","), topic))
      else {
        if (partitionsToBeAdded.nonEmpty) {
          info("New partitions to be added %s".format(partitionsToBeAdded))
          controllerContext.partitionReplicaAssignment.++=(partitionsToBeAdded)
          //切换到新增分区以及副本的状态
          onNewPartitionCreation(partitionsToBeAdded.keySet)
        }
      }
    }
  }

  case class TopicDeletion(var topicsToBeDeleted: Set[String]) extends ControllerEvent {

    def state = ControllerState.TopicDeletion

    override def process(): Unit = {
      if (!isActive) return
      debug("Delete topics listener fired for topics %s to be deleted".format(topicsToBeDeleted.mkString(",")))
      val nonExistentTopics = topicsToBeDeleted -- controllerContext.allTopics
      if (nonExistentTopics.nonEmpty) {
        warn("Ignoring request to delete non-existing topics " + nonExistentTopics.mkString(","))
        nonExistentTopics.foreach(topic => zkUtils.deletePathRecursive(getDeleteTopicPath(topic)))
      }
      topicsToBeDeleted --= nonExistentTopics
      if (config.deleteTopicEnable) {
        if (topicsToBeDeleted.nonEmpty) {
          info("Starting topic deletion for topics " + topicsToBeDeleted.mkString(","))
          // mark topic ineligible for deletion if other state changes are in progress
          topicsToBeDeleted.foreach { topic =>
            val partitionReassignmentInProgress =
              controllerContext.partitionsBeingReassigned.keySet.map(_.topic).contains(topic)
            if (partitionReassignmentInProgress)
              topicDeletionManager.markTopicIneligibleForDeletion(Set(topic))
          }
          // add topic to deletion list
          //将删除的分区放入topicsToBeDeleted
          topicDeletionManager.enqueueTopicsForDeletion(topicsToBeDeleted)
        }
      } else {
        // If delete topic is disabled remove entries under zookeeper path : /admin/delete_topics
        for (topic <- topicsToBeDeleted) {
          info("Removing " + getDeleteTopicPath(topic) + " since delete topic is disabled")
          zkUtils.zkClient.delete(getDeleteTopicPath(topic))
        }
      }
    }
  }

  case class PartitionReassignment(partitionReassignment: Map[TopicAndPartition, Seq[Int]]) extends ControllerEvent {

    def state = ControllerState.PartitionReassignment

    override def process(): Unit = {
      if (!isActive) return
      val partitionsToBeReassigned = partitionReassignment.filterNot(p => controllerContext.partitionsBeingReassigned.contains(p._1))
      partitionsToBeReassigned.foreach { partitionToBeReassigned =>
        if(topicDeletionManager.isTopicQueuedUpForDeletion(partitionToBeReassigned._1.topic)) {
          error("Skipping reassignment of partition %s for topic %s since it is currently being deleted"
            .format(partitionToBeReassigned._1, partitionToBeReassigned._1.topic))
          removePartitionFromReassignedPartitions(partitionToBeReassigned._1)
        } else {
          val context = ReassignedPartitionsContext(partitionToBeReassigned._2)
          initiateReassignReplicasForTopicPartition(partitionToBeReassigned._1, context)
        }
      }
    }

  }

  case class PartitionReassignmentIsrChange(topicAndPartition: TopicAndPartition, reassignedReplicas: Set[Int]) extends ControllerEvent {

    def state = ControllerState.PartitionReassignment

    override def process(): Unit = {
      if (!isActive) return
        // check if this partition is still being reassigned or not
      controllerContext.partitionsBeingReassigned.get(topicAndPartition) match {
        case Some(reassignedPartitionContext) =>
          // need to re-read leader and isr from zookeeper since the zkclient callback doesn't return the Stat object
          val newLeaderAndIsrOpt = zkUtils.getLeaderAndIsrForPartition(topicAndPartition.topic, topicAndPartition.partition)
          newLeaderAndIsrOpt match {
            case Some(leaderAndIsr) => // check if new replicas have joined ISR
              val caughtUpReplicas = reassignedReplicas & leaderAndIsr.isr.toSet
              if(caughtUpReplicas == reassignedReplicas) {
                // resume the partition reassignment process
                info("%d/%d replicas have caught up with the leader for partition %s being reassigned."
                  .format(caughtUpReplicas.size, reassignedReplicas.size, topicAndPartition) +
                  "Resuming partition reassignment")
                onPartitionReassignment(topicAndPartition, reassignedPartitionContext)
              }
              else {
                info("%d/%d replicas have caught up with the leader for partition %s being reassigned."
                  .format(caughtUpReplicas.size, reassignedReplicas.size, topicAndPartition) +
                  "Replica(s) %s still need to catch up".format((reassignedReplicas -- leaderAndIsr.isr.toSet).mkString(",")))
              }
            case None => error("Error handling reassignment of partition %s to replicas %s as it was never created"
              .format(topicAndPartition, reassignedReplicas.mkString(",")))
          }
        case None =>
      }
    }
  }

  case class IsrChangeNotification(sequenceNumbers: Seq[String]) extends ControllerEvent {

    def state = ControllerState.IsrChange

    override def process(): Unit = {
      if (!isActive) return
      try {
        val topicAndPartitions = sequenceNumbers.flatMap(getTopicAndPartition).toSet
        if (topicAndPartitions.nonEmpty) {
          // 从zk中读取指定分区leader副本，isr集合等信息，更新ControllerContex
          updateLeaderAndIsrCache(topicAndPartitions)
          // 向可用Broker发送UpdateMetadataReques，更新MetadataCache
          processUpdateNotifications(topicAndPartitions)
        }
      } finally {
        // delete the notifications
        // 删除已处理的消息
        sequenceNumbers.map(x => controllerContext.zkUtils.deletePath(ZkUtils.IsrChangeNotificationPath + "/" + x))
      }
    }

    private def processUpdateNotifications(topicAndPartitions: immutable.Set[TopicAndPartition]) {
      val liveBrokers: Seq[Int] = controllerContext.liveOrShuttingDownBrokerIds.toSeq
      debug("Sending MetadataRequest to Brokers:" + liveBrokers + " for TopicAndPartitions:" + topicAndPartitions)
      sendUpdateMetadataRequest(liveBrokers, topicAndPartitions)
    }

    private def getTopicAndPartition(child: String): Set[TopicAndPartition] = {
      val changeZnode: String = ZkUtils.IsrChangeNotificationPath + "/" + child
      val (jsonOpt, _) = controllerContext.zkUtils.readDataMaybeNull(changeZnode)
      if (jsonOpt.isDefined) {
        val json = Json.parseFull(jsonOpt.get)

        json match {
          case Some(m) =>
            val topicAndPartitions: mutable.Set[TopicAndPartition] = new mutable.HashSet[TopicAndPartition]()
            val isrChanges = m.asInstanceOf[Map[String, Any]]
            val topicAndPartitionList = isrChanges("partitions").asInstanceOf[List[Any]]
            topicAndPartitionList.foreach {
              case tp =>
                val topicAndPartition = tp.asInstanceOf[Map[String, Any]]
                val topic = topicAndPartition("topic").asInstanceOf[String]
                val partition = topicAndPartition("partition").asInstanceOf[Int]
                topicAndPartitions += TopicAndPartition(topic, partition)
            }
            topicAndPartitions
          case None =>
            error("Invalid topic and partition JSON: " + jsonOpt.get + " in ZK: " + changeZnode)
            Set.empty
        }
      } else {
        Set.empty
      }
    }

  }

  case class PreferredReplicaLeaderElection(partitions: Set[TopicAndPartition]) extends ControllerEvent {

    def state = ControllerState.ManualLeaderBalance

    override def process(): Unit = {
      if (!isActive) return
      //过滤掉待删除Topic分区
      val partitionsForTopicsToBeDeleted = partitions.filter(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))
      if (partitionsForTopicsToBeDeleted.nonEmpty) {
        error("Skipping preferred replica election for partitions %s since the respective topics are being deleted"
          .format(partitionsForTopicsToBeDeleted))
      }
      // 对剩余分区进行优先副本选举
      onPreferredReplicaElection(partitions -- partitionsForTopicsToBeDeleted)
    }

  }

  case object AutoPreferredReplicaLeaderElection extends ControllerEvent {

    def state = ControllerState.AutoLeaderBalance

    override def process(): Unit = {
      if (!isActive) return
      try {
        checkAndTriggerAutoLeaderRebalance()
      } finally {
        scheduleAutoLeaderRebalanceTask(delay = config.leaderImbalanceCheckIntervalSeconds, unit = TimeUnit.SECONDS)
      }
    }
  }

  case class ControlledShutdown(id: Int, controlledShutdownCallback: Try[Set[TopicAndPartition]] => Unit) extends ControllerEvent {

    def state = ControllerState.ControlledShutdown

    override def process(): Unit = {
      val controlledShutdownResult = Try { doControlledShutdown(id) }
      controlledShutdownCallback(controlledShutdownResult)
    }

    private def doControlledShutdown(id: Int): Set[TopicAndPartition] = {
      if (!isActive) {
        throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown")
      }

      info("Shutting down broker " + id)

      if (!controllerContext.liveOrShuttingDownBrokerIds.contains(id))
        throw new BrokerNotAvailableException("Broker id %d does not exist.".format(id))

      controllerContext.shuttingDownBrokerIds.add(id)
      debug("All shutting down brokers: " + controllerContext.shuttingDownBrokerIds.mkString(","))
      debug("Live brokers: " + controllerContext.liveBrokerIds.mkString(","))
      // 获得待关闭Broker上所有Parition和副本信息
      val allPartitionsAndReplicationFactorOnBroker: Set[(TopicAndPartition, Int)] =
          controllerContext.partitionsOnBroker(id)
            .map(topicAndPartition => (topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition).size))

      allPartitionsAndReplicationFactorOnBroker.foreach { case (topicAndPartition, replicationFactor) =>
        controllerContext.partitionLeadershipInfo.get(topicAndPartition).foreach { currLeaderIsrAndControllerEpoch =>
          // 是否开启副本机制
          if (replicationFactor > 1) {
            // 将相关的Parition切换为OnlineParition状态，使用ControlledShutdownLeaderSelector重新选举Leader和isr集合，并将结果写入zk
            if (currLeaderIsrAndControllerEpoch.leaderAndIsr.leader == id) {
              // If the broker leads the topic partition, transition the leader and update isr. Updates zk and
              // notifies all affected brokers
              partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition,
                controlledShutdownPartitionLeaderSelector)
            } else {
              // Stop the replica first. The state change below initiates ZK changes which should take some time
              // before which the stop replica request should be completed (in most cases)
              try {
                // 发送StopReplicaRequest
                brokerRequestBatch.newBatch()
                brokerRequestBatch.addStopReplicaRequestForBrokers(Seq(id), topicAndPartition.topic,
                  topicAndPartition.partition, deletePartition = false)
                brokerRequestBatch.sendRequestsToBrokers(epoch)
              } catch {
                case e: IllegalStateException =>
                  handleIllegalState(e)
              }
              // If the broker is a follower, updates the isr in ZK and notifies the current leader
              // 将副本状态换成OfflineReplica状态
              replicaStateMachine.handleStateChanges(Set(PartitionAndReplica(topicAndPartition.topic,
                topicAndPartition.partition, id)), OfflineReplica)
            }
          }
        }
      }
      // 统计Leader副本依然处于待关闭Broker上的分区，并将其返回
      def replicatedPartitionsBrokerLeads() = {
        trace("All leaders = " + controllerContext.partitionLeadershipInfo.mkString(","))
        controllerContext.partitionLeadershipInfo.filter {
          case (topicAndPartition, leaderIsrAndControllerEpoch) =>
            leaderIsrAndControllerEpoch.leaderAndIsr.leader == id && controllerContext.partitionReplicaAssignment(topicAndPartition).size > 1
        }.keys
      }
      replicatedPartitionsBrokerLeads().toSet
    }
  }

  case class TopicDeletionStopReplicaResult(stopReplicaResponseObj: AbstractResponse, replicaId: Int) extends ControllerEvent {

    def state = ControllerState.TopicDeletion

    override def process(): Unit = {
      import JavaConverters._
      if (!isActive) return
      val stopReplicaResponse = stopReplicaResponseObj.asInstanceOf[StopReplicaResponse]
      debug("Delete topic callback invoked for %s".format(stopReplicaResponse))
      val responseMap = stopReplicaResponse.responses.asScala
      val partitionsInError =
        if (stopReplicaResponse.error != Errors.NONE) responseMap.keySet
        else responseMap.filter { case (_, error) => error != Errors.NONE }.keySet
      val replicasInError = partitionsInError.map(p => PartitionAndReplica(p.topic, p.partition, replicaId))
      // move all the failed replicas to ReplicaDeletionIneligible
      topicDeletionManager.failReplicaDeletion(replicasInError)
      if (replicasInError.size != responseMap.size) {
        // some replicas could have been successfully deleted
        val deletedReplicas = responseMap.keySet -- partitionsInError
        topicDeletionManager.completeReplicaDeletion(deletedReplicas.map(p => PartitionAndReplica(p.topic, p.partition, replicaId)))
      }
    }
  }

  case object Startup extends ControllerEvent {

    def state = ControllerState.ControllerChange

    override def process(): Unit = {
      // 注册SessionExpirationListener
      registerSessionExpirationListener()
      // 注册ControllerChangeListener
      registerControllerChangeListener()
      elect()
    }

  }

  case class ControllerChange(newControllerId: Int) extends ControllerEvent {

    def state = ControllerState.ControllerChange

    override def process(): Unit = {
      val wasActiveBeforeChange = isActive
      activeControllerId = newControllerId
      if (wasActiveBeforeChange && !isActive) {
        onControllerResignation()
      }
    }

  }

  case object Reelect extends ControllerEvent {

    def state = ControllerState.ControllerChange

    override def process(): Unit = {
      val wasActiveBeforeChange = isActive
      activeControllerId = getControllerID()
      if (wasActiveBeforeChange && !isActive) {
        onControllerResignation()
      }
      elect()
    }

  }

  private def updateMetrics(): Unit = {
    offlinePartitionCount =
      if (!isActive) 0
      else {
        controllerContext.partitionLeadershipInfo.count { case (tp, leadershipInfo) =>
          !controllerContext.liveOrShuttingDownBrokerIds.contains(leadershipInfo.leaderAndIsr.leader) &&
            !topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic)
        }
      }

    preferredReplicaImbalanceCount =
      if (!isActive) 0
      else {
        controllerContext.partitionReplicaAssignment.count { case (topicPartition, replicas) =>
          val preferredReplica = replicas.head
          val leadershipInfo = controllerContext.partitionLeadershipInfo.get(topicPartition)
          leadershipInfo.map(_.leaderAndIsr.leader != preferredReplica).getOrElse(false) &&
            !topicDeletionManager.isTopicQueuedUpForDeletion(topicPartition.topic)
        }
      }
  }

  // visible for testing
  private[controller] def handleIllegalState(e: IllegalStateException): Nothing = {
    // Resign if the controller is in an illegal state
    error("Forcing the controller to resign")
    brokerRequestBatch.clear()
    triggerControllerMove()
    throw e
  }

  private def triggerControllerMove(): Unit = {
    onControllerResignation()
    activeControllerId = -1
    controllerContext.zkUtils.deletePath(ZkUtils.ControllerPath)
  }

  def elect(): Unit = {
    val timestamp = time.milliseconds
    val electString = ZkUtils.controllerZkData(config.brokerId, timestamp)

    activeControllerId = getControllerID()
    /*
     * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition,
     * it's possible that the controller has already been elected when we get here. This check will prevent the following
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     */
    if (activeControllerId != -1) {
      debug("Broker %d has been elected as the controller, so stopping the election process.".format(activeControllerId))
      return
    }

    try {
      val zkCheckedEphemeral = new ZKCheckedEphemeral(ZkUtils.ControllerPath,
                                                      electString,
                                                      controllerContext.zkUtils.zkConnection.getZookeeper,
                                                       controllerContext.zkUtils.isSecure)
      // 尝试创建临时节点，如果临时节点已经存在，则抛出异常
      zkCheckedEphemeral.create()
      info(config.brokerId + " successfully elected as the controller")

      // 更新activeControllerId，当broker成为controller leader
      activeControllerId = config.brokerId

      // 启动parition-rebalance定时任务，提供了分区自动平衡功能
      onControllerFailover()
    } catch {
      case _: ZkNodeExistsException =>
        // If someone else has written the path, then
        activeControllerId = getControllerID

        if (activeControllerId != -1)
          debug("Broker %d was elected as controller instead of broker %d".format(activeControllerId, config.brokerId))
        else
          warn("A controller has been elected but just resigned, this will result in another round of election")

      case e2: Throwable =>
        error("Error while electing or becoming controller on broker %d".format(config.brokerId), e2)
        triggerControllerMove()
    }
  }
}

/**
  * This is the zookeeper listener that triggers all the state transitions for a replica
  */
//用于监听/brokers/ids节点下的子节点变化，负责处理broker的上线和故障下线
class BrokerChangeListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkChildListener with Logging {
  override def handleChildChange(parentPath: String, currentChilds: java.util.List[String]): Unit = {
    import JavaConverters._
    eventManager.put(controller.BrokerChange(currentChilds.asScala))
  }
}
//负责topic的删减
class TopicChangeListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkChildListener with Logging {
  override def handleChildChange(parentPath: String, currentChilds: java.util.List[String]): Unit = {
    import JavaConverters._
    eventManager.put(controller.TopicChange(currentChilds.asScala.toSet))
  }
}
//监听topic的变化
class PartitionModificationsListener(controller: KafkaController, eventManager: ControllerEventManager, topic: String) extends IZkDataListener with Logging {
  override def handleDataChange(dataPath: String, data: Any): Unit = {
    eventManager.put(controller.PartitionModifications(topic))
  }

  override def handleDataDeleted(dataPath: String): Unit = {}
}

/**
  * Delete topics includes the following operations -
  * 1. Add the topic to be deleted to the delete topics cache, only if the topic exists
  * 2. If there are topics to be deleted, it signals the delete topic thread
  */
//删除topic
class TopicDeletionListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkChildListener with Logging {
  override def handleChildChange(parentPath: String, currentChilds: java.util.List[String]): Unit = {
    import JavaConverters._
    eventManager.put(controller.TopicDeletion(currentChilds.asScala.toSet))
  }
}

/**
 * Starts the partition reassignment process unless -
 * 1. Partition previously existed
 * 2. New replicas are the same as existing replicas
 * 3. Any replica in the new set of replicas are dead
 * If any of the above conditions are satisfied, it logs an error and removes the partition from list of reassigned
 * partitions.
 */
class PartitionReassignmentListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkDataListener with Logging {
  override def handleDataChange(dataPath: String, data: Any): Unit = {
    val partitionReassignment = ZkUtils.parsePartitionReassignmentData(data.toString)
    eventManager.put(controller.PartitionReassignment(partitionReassignment))
  }

  override def handleDataDeleted(dataPath: String): Unit = {}
}

class PartitionReassignmentIsrChangeListener(controller: KafkaController, eventManager: ControllerEventManager,
                                             topic: String, partition: Int, reassignedReplicas: Set[Int]) extends IZkDataListener with Logging {
  override def handleDataChange(dataPath: String, data: Any): Unit = {
    eventManager.put(controller.PartitionReassignmentIsrChange(TopicAndPartition(topic, partition), reassignedReplicas))
  }

  override def handleDataDeleted(dataPath: String): Unit = {}
}

/**
 * Called when replica leader initiates isr change
 */
// 当某些分区的isr集合发生变化时候。通知整个集群中的所有Broker
class IsrChangeNotificationListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkChildListener with Logging {
  override def handleChildChange(parentPath: String, currentChilds: java.util.List[String]): Unit = {
    import JavaConverters._
    eventManager.put(controller.IsrChangeNotification(currentChilds.asScala))
  }
}

object IsrChangeNotificationListener {
  val version: Long = 1L
}

/**
 * Starts the preferred replica leader election for the list of partitions specified under
 * /admin/preferred_replica_election -
 */
// 优先选举时候，监听/admin/preferred_replica_election节点
class PreferredReplicaElectionListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkDataListener with Logging {
  override def handleDataChange(dataPath: String, data: Any): Unit = {
    val partitions = PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(data.toString)
    eventManager.put(controller.PreferredReplicaLeaderElection(partitions))
  }

  override def handleDataDeleted(dataPath: String): Unit = {}
}

class ControllerChangeListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkDataListener {
  override def handleDataChange(dataPath: String, data: Any): Unit = {
    eventManager.put(controller.ControllerChange(KafkaController.parseControllerId(data.toString)))
  }

  override def handleDataDeleted(dataPath: String): Unit = {
    eventManager.put(controller.Reelect)
  }
}

class SessionExpirationListener(controller: KafkaController, eventManager: ControllerEventManager) extends IZkStateListener with Logging {
  override def handleStateChanged(state: KeeperState) {
    // do nothing, since zkclient will do reconnect for us.
  }

  /**
    * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
    * any ephemeral nodes here.
    *
    * @throws Exception On any error.
    */
  @throws[Exception]
  override def handleNewSession(): Unit = {
    eventManager.put(controller.Reelect)
  }

  override def handleSessionEstablishmentError(error: Throwable): Unit = {
    //no-op handleSessionEstablishmentError in KafkaHealthCheck should handle this error in its handleSessionEstablishmentError
  }
}

case class ReassignedPartitionsContext(var newReplicas: Seq[Int] = Seq.empty,
                                       var isrChangeListener: PartitionReassignmentIsrChangeListener = null)

case class PartitionAndReplica(topic: String, partition: Int, replica: Int) {
  override def toString: String = {
    "[Topic=%s,Partition=%d,Replica=%d]".format(topic, partition, replica)
  }
}

case class LeaderIsrAndControllerEpoch(leaderAndIsr: LeaderAndIsr, controllerEpoch: Int) {
  override def toString: String = {
    val leaderAndIsrInfo = new StringBuilder
    leaderAndIsrInfo.append("(Leader:" + leaderAndIsr.leader)
    leaderAndIsrInfo.append(",ISR:" + leaderAndIsr.isr.mkString(","))
    leaderAndIsrInfo.append(",LeaderEpoch:" + leaderAndIsr.leaderEpoch)
    leaderAndIsrInfo.append(",ControllerEpoch:" + controllerEpoch + ")")
    leaderAndIsrInfo.toString()
  }
}

private[controller] class ControllerStats extends KafkaMetricsGroup {
  val uncleanLeaderElectionRate = newMeter("UncleanLeaderElectionsPerSec", "elections", TimeUnit.SECONDS)

  val rateAndTimeMetrics: Map[ControllerState, KafkaTimer] = ControllerState.values.flatMap { state =>
    state.rateAndTimeMetricName.map { metricName =>
      state -> new KafkaTimer(newTimer(s"$metricName", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
    }
  }.toMap

}

sealed trait ControllerEvent {
  def state: ControllerState
  def process(): Unit
}
