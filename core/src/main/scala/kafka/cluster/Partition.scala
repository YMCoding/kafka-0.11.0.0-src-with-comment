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
package kafka.cluster

import java.io.IOException
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.yammer.metrics.core.Gauge
import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.common.NotAssignedReplicaException
import kafka.controller.KafkaController
import kafka.log.LogConfig
import kafka.metrics.KafkaMetricsGroup
import kafka.server._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{NotEnoughReplicasException, NotLeaderForPartitionException, PolicyViolationException}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.{EpochEndOffset, PartitionState}
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 */
//用来管理对应Replica
class Partition(val topic: String,//Topic
                val partitionId: Int,//Partition
                time: Time,
                replicaManager: ReplicaManager) extends Logging with KafkaMetricsGroup {
  val topicPartition = new TopicPartition(topic, partitionId)

  private val localBrokerId = replicaManager.config.brokerId
  //当前Broker的LogManager对象
  private val logManager = replicaManager.logManager
  //操作Zookeeper
  private val zkUtils = replicaManager.zkUtils
  private val assignedReplicaMap = new Pool[Int, Replica]
  // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  //Leader副本的年代信息
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  //分区Leader副本的id
  @volatile var leaderReplicaIdOpt: Option[Int] = None
  //ISR集合
  @volatile var inSyncReplicas: Set[Replica] = Set.empty[Replica]

  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  this.logIdent = "Partition [%s,%d] on broker %d: ".format(topic, partitionId, localBrokerId)

  private def isReplicaLocal(replicaId: Int) : Boolean = replicaId == localBrokerId
  val tags = Map("topic" -> topic, "partition" -> partitionId.toString)

  newGauge("UnderReplicated",
    new Gauge[Int] {
      def value = {
        if (isUnderReplicated) 1 else 0
      }
    },
    tags
  )

  newGauge("InSyncReplicasCount",
    new Gauge[Int] {
      def value = {
        if (isLeaderReplicaLocal) inSyncReplicas.size else 0
      }
    },
    tags
  )

  newGauge("ReplicasCount",
    new Gauge[Int] {
      def value = {
        if (isLeaderReplicaLocal) assignedReplicas.size else 0
      }
    },
    tags
  )

  newGauge("LastStableOffsetLag",
    new Gauge[Long] {
      def value = {
        leaderReplicaIfLocal.map { replica =>
          replica.highWatermark.messageOffset - replica.lastStableOffset.messageOffset
        }.getOrElse(0)
      }
    },
    tags
  )

  private def isLeaderReplicaLocal: Boolean = leaderReplicaIfLocal.isDefined

  def isUnderReplicated: Boolean =
    isLeaderReplicaLocal && inSyncReplicas.size < assignedReplicas.size

  //在ar集合中查找指定的副本的Replic对象，没有则创建田间
  def getOrCreateReplica(replicaId: Int = localBrokerId): Replica = {
    //查找
    assignedReplicaMap.getAndMaybePut(replicaId, {
      //本地Replica对象，更新hw
      if (isReplicaLocal(replicaId)) {
        //获取配置
        val config = LogConfig.fromProps(logManager.defaultConfig.originals,
                                         AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, topic))
        //创建Local Replica对应的log，如果存在，则直接返回
        val log = logManager.createLog(topicPartition, config)

        //获得指定的log目录对应的OffsetCheckpoint对象，管理目录下的replication-offset-checkpoint文件
        val checkpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParentFile.getAbsolutePath)

        //hw形成map
        val offsetMap = checkpoint.read
        if (!offsetMap.contains(topicPartition))
          info(s"No checkpointed highwatermark is found for partition $topicPartition")

        //根据TopicAndPartition找到对应的Hw,和Leo比较
        val offset = math.min(offsetMap.getOrElse(topicPartition, 0L), log.logEndOffset)

        //创建Replic对象，添加到assignedReplicaMap集合中管理
        new Replica(replicaId, this, time, offset, Some(log))
      } else new Replica(replicaId, this, time)
    })
  }
  //找到对应的Replica
  def getReplica(replicaId: Int = localBrokerId): Option[Replica] = Option(assignedReplicaMap.get(replicaId))

  def leaderReplicaIfLocal: Option[Replica] =
    leaderReplicaIdOpt.filter(_ == localBrokerId).flatMap(getReplica)

  def addReplicaIfNotExists(replica: Replica): Replica =
    assignedReplicaMap.putIfNotExists(replica.brokerId, replica)

  def assignedReplicas: Set[Replica] =
    assignedReplicaMap.values.toSet

  //移除
  private def removeReplica(replicaId: Int) {
    assignedReplicaMap.remove(replicaId)
  }

  def delete() {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) {
      assignedReplicaMap.clear()
      inSyncReplicas = Set.empty[Replica]
      leaderReplicaIdOpt = None
      try {
        logManager.asyncDelete(topicPartition)
        removePartitionMetrics()
      } catch {
        case e: IOException =>
          fatal(s"Error deleting the log for partition $topicPartition", e)
          Exit.halt(1)
      }
    }
  }

  def getLeaderEpoch: Int = this.leaderEpoch

  /**
   * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset
   * from the time when this broker was the leader last time) and setting the new leader and ISR.
   * If the leader replica id does not change, return false to indicate the replica manager.
   */
  //副本角色切换,选leader
  def makeLeader(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    //加锁
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {

      //获取需要分配的ar集合
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path

      //记录controllerEpoch
      controllerEpoch = partitionStateInfo.controllerEpoch
      // add replicas that are new
      //获得isr集合
      val newInSyncReplicas = partitionStateInfo.isr.asScala.map(r => getOrCreateReplica(r)).toSet

      //根据allReplicas更新assignedReplicas
      (assignedReplicas.map(_.brokerId) -- allReplicas).foreach(removeReplica)

      //更新isr集合
      inSyncReplicas = newInSyncReplicas

      info(s"$topicPartition starts at Leader Epoch ${partitionStateInfo.leaderEpoch} from offset ${getReplica().get.logEndOffset.messageOffset}. Previous Leader Epoch was: $leaderEpoch")

      //We cache the leader epoch here, persisting it only if it's local (hence having a log dir)
      //更新leaderEpoch
      leaderEpoch = partitionStateInfo.leaderEpoch

      //创建ar集合中所有副本对应的Replica对象
      allReplicas.foreach(id => getOrCreateReplica(id))

      //更新version
      zkVersion = partitionStateInfo.zkVersion

      //检测Leader是否发生变化
      val isNewLeader =
        if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == localBrokerId) {
          //没有发生变化
          false
        } else {
          //发生变化，即leader不在这个broker上
          leaderReplicaIdOpt = Some(localBrokerId)
          true
        }
      //获取local replica
      val leaderReplica = getReplica().get
      val curLeaderLogEndOffset = leaderReplica.logEndOffset.messageOffset
      val curTimeMs = time.milliseconds
      // initialize lastCaughtUpTime of replicas as well as their lastFetchTimeMs and lastFetchLeaderLogEndOffset.
      (assignedReplicas - leaderReplica).foreach { replica =>
        val lastCaughtUpTimeMs = if (inSyncReplicas.contains(replica)) curTimeMs else 0L
        replica.resetLastCaughtUpTime(curLeaderLogEndOffset, curTimeMs, lastCaughtUpTimeMs)
      }

      // we may need to increment high watermark since ISR could be down to 1
      //新节点
      if (isNewLeader) {

        // construct the high watermark metadata for the new leader replica
        //通过log.read实现
        leaderReplica.convertHWToLocalOffsetMetadata()

        // reset log end offset for remote replicas
        //重置Remoto Replica的leo的值为-1
        assignedReplicas.filter(_.brokerId != localBrokerId).foreach(_.updateLogReadResult(LogReadResult.UnknownLogReadResult))
      }
      //尝试更新HW
      //isr集合发生增减或者是sir集合中任一副本吧发生变化，都会导致isr集合中最小的leo变大
      (maybeIncrementLeaderHW(leaderReplica), isNewLeader)
    }

    // some delayed operations may be unblocked after HW changed
    //如果HW增减了，则DelayedFetch满足执行条
    if (leaderHWIncremented)
      //检查delayedProducePuragatory
      tryCompleteDelayedRequests()
    isNewLeader
  }

  /**
   *  Make the local replica the follower by setting the new leader and ISR to empty
   *  If the leader replica id does not change, return false to indicate the replica manager
   */
  //根据partitionState指定的信息，将local replica设置为follower的副本
  def makeFollower(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    //加锁
    inWriteLock(leaderIsrUpdateLock) {
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      val newLeaderBrokerId: Int = partitionStateInfo.leader
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.controllerEpoch

      // add replicas that are new
      //创建对应的Replica对象
      allReplicas.foreach(r => getOrCreateReplica(r))
      // remove assigned replicas that have been removed by the controller
      (assignedReplicas.map(_.brokerId) -- allReplicas).foreach(removeReplica)

      //空集合，isr集合在leader副本上进行维护，在follower上不维护isr集合
      inSyncReplicas = Set.empty[Replica]
      leaderEpoch = partitionStateInfo.leaderEpoch
      zkVersion = partitionStateInfo.zkVersion

      //检测leader是否发生变化
      if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == newLeaderBrokerId) {
        false
      }
      else {
        //更新leaderReplicaIdOpt
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  /**
   * Update the log end offset of a certain replica of this partition
   */
  def updateReplicaLogReadResult(replicaId: Int, logReadResult: LogReadResult) {
    getReplica(replicaId) match {
      case Some(replica) =>
        // No need to calculate low watermark if there is no delayed DeleteRecordsRequest
        val oldLeaderLW = if (replicaManager.delayedDeleteRecordsPurgatory.delayed > 0) lowWatermarkIfLeader else -1L

        //更新follower副本的状态
        replica.updateLogReadResult(logReadResult)
        val newLeaderLW = if (replicaManager.delayedDeleteRecordsPurgatory.delayed > 0) lowWatermarkIfLeader else -1L
        // check if the LW of the partition has incremented
        // since the replica's logStartOffset may have incremented
        val leaderLWIncremented = newLeaderLW > oldLeaderLW
        // check if we need to expand ISR to include this replica
        // if it is not in the ISR yet

        //扩展isr集合
        val leaderHWIncremented = maybeExpandIsr(replicaId, logReadResult)

        // some delayed operations may be unblocked after HW or LW changed
        if (leaderLWIncremented || leaderHWIncremented)
          tryCompleteDelayedRequests()

        debug("Recorded replica %d log end offset (LEO) position %d."
          .format(replicaId, logReadResult.info.fetchOffsetMetadata.messageOffset))
      case None =>
        throw new NotAssignedReplicaException(("Leader %d failed to record follower %d's position %d since the replica" +
          " is not recognized to be one of the assigned replicas %s for partition %s.")
          .format(localBrokerId,
                  replicaId,
                  logReadResult.info.fetchOffsetMetadata.messageOffset,
                  assignedReplicas.map(_.brokerId).mkString(","),
                  topicPartition))
    }
  }

  /**
   * Check and maybe expand the ISR of the partition.
   * A replica will be added to ISR if its LEO >= current hw of the partition.
   *
   * Technically, a replica shouldn't be in ISR if it hasn't caught up for longer than replicaLagTimeMaxMs,
   * even if its log end offset is >= HW. However, to be consistent with how the follower determines
   * whether a replica is in-sync, we only check HW.
   *
   * This function can be triggered when a replica's LEO has incremented
   */
  //管理isr集合
  def maybeExpandIsr(replicaId: Int, logReadResult: LogReadResult): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {

      // check if this replica needs to be added to the ISR
      //只有leader才会管理isr现获取leader副本的replica对象
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val replica = getReplica(replicaId).get

          //获取HW
          val leaderHW = leaderReplica.highWatermark
          if(!inSyncReplicas.contains(replica) && //follower副本不在isr集合中
             assignedReplicas.map(_.brokerId).contains(replicaId) && //ar集合中可以查找到
             replica.logEndOffset.offsetDiff(leaderHW) >= 0) { //follower副本的leo已经追赶上hw

            //将改follower副本添加到isr集合
            val newInSyncReplicas = inSyncReplicas + replica
            info(s"Expanding ISR from ${inSyncReplicas.map(_.brokerId).mkString(",")} " +
              s"to ${newInSyncReplicas.map(_.brokerId).mkString(",")}")

            // update ISR in ZK and cache
            //将isr集合的信息上传到zookeeper中保存
            updateIsr(newInSyncReplicas)

            // 标识发生了一次isr集合扩张
            replicaManager.isrExpandRate.mark()
          }
          // check if the HW of the partition can now be incremented
          // since the replica may already be in the ISR and its LEO has just incremented
          //更新hw
          maybeIncrementLeaderHW(leaderReplica, logReadResult.fetchTimeMs)
        case None => false // nothing to do if no longer leader
      }
    }
  }

  /*
   * Returns a tuple where the first element is a boolean indicating whether enough replicas reached `requiredOffset`
   * and the second element is an error (which would be `Errors.NONE` for no error).
   *
   * Note that this method will only be called if requiredAcks = -1 and we are waiting for all replicas in ISR to be
   * fully caught up to the (local) leader's offset corresponding to this produce request before we acknowledge the
   * produce request.
   */
  //指定的消息是否已经被isr集合中所有的follower副本同步
  def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Errors) = {
    //获得leader副本中对应的replica
    leaderReplicaIfLocal match {
      case Some(leaderReplica) =>

        // keep the current immutable replica list reference
        //获得当前的isr集合
        val curInSyncReplicas = inSyncReplicas

        //检测ack
        def numAcks = curInSyncReplicas.count { r =>
          if (!r.isLocal)
            if (r.logEndOffset.messageOffset >= requiredOffset) {
              trace(s"Replica ${r.brokerId} received offset $requiredOffset")
              true
            }
            else
              false
          else
            true /* also count the local (leader) replica */
        }

        trace(s"$numAcks acks satisfied with acks = -1")

        val minIsr = leaderReplica.log.get.config.minInSyncReplicas

        //比较hw与消息得offset
        if (leaderReplica.highWatermark.messageOffset >= requiredOffset) {
          /*
           * The topic may be configured not to accept messages if there are not enough replicas in ISR
           * in this scenario the request was already appended locally and then added to the purgatory before the ISR was shrunk
           */
          if (minIsr <= curInSyncReplicas.size)
            (true, Errors.NONE)
          else
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
        } else
          (false, Errors.NONE)
      case None =>
        (false, Errors.NOT_LEADER_FOR_PARTITION)
    }
  }

  /**
   * Check and maybe increment the high watermark of the partition;
   * this function can be triggered when
   *
   * 1. Partition ISR changed
   * 2. Any replica's LEO changed
   *
   * The HW is determined by the smallest log end offset among all replicas that are in sync or are considered caught-up.
   * This way, if a replica is considered caught-up, but its log end offset is smaller than HW, we will wait for this
   * replica to catch up to the HW before advancing the HW. This helps the situation when the ISR only includes the
   * leader replica and a follower tries to catch up. If we don't wait for the follower when advancing the HW, the
   * follower's log end offset may keep falling behind the HW (determined by the leader's log end offset) and therefore
   * will never be added to ISR.
   *
   * Returns true if the HW was incremented, and false otherwise.
   * Note There is no need to acquire the leaderIsrUpdate lock here
   * since all callers of this private API acquire that lock
   */
  private def maybeIncrementLeaderHW(leaderReplica: Replica, curTime: Long = time.milliseconds): Boolean = {

    //获得所有的leo
    val allLogEndOffsets = assignedReplicas.filter { replica =>
      curTime - replica.lastCaughtUpTimeMs <= replicaManager.config.replicaLagTimeMaxMs || inSyncReplicas.contains(replica)
    }.map(_.logEndOffset)

    //集合中最小的leo作为新的hw
    val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering)
    val oldHighWatermark = leaderReplica.highWatermark

    //比较新旧两个hw值，决定是否更新hw
    if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset || oldHighWatermark.onOlderSegment(newHighWatermark)) {
      //更新
      leaderReplica.highWatermark = newHighWatermark
      debug(s"High watermark updated to $newHighWatermark")
      true
    } else  {
      debug(s"Skipping update high watermark since new hw $newHighWatermark is not larger than old hw $oldHighWatermark." +
        s"All LEOs are ${allLogEndOffsets.mkString(",")}")
      false
    }
  }

  /**
   * The low watermark offset value, calculated only if the local replica is the partition leader
   * It is only used by leader broker to decide when DeleteRecordsRequest is satisfied. Its value is minimum logStartOffset of all live replicas
   * Low watermark will increase when the leader broker receives either FetchRequest or DeleteRecordsRequest.
   */
  def lowWatermarkIfLeader: Long = {
    if (!isLeaderReplicaLocal)
      throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d".format(topicPartition, localBrokerId))
    assignedReplicas.filter(replica =>
      replicaManager.metadataCache.isBrokerAlive(replica.brokerId)).map(_.logStartOffset).reduceOption(_ min _).getOrElse(0L)
  }

  /**
   * Try to complete any pending requests. This should be called without holding the leaderIsrUpdateLock.
   */
  private def tryCompleteDelayedRequests() {
    val requestKey = new TopicPartitionOperationKey(topicPartition)
    replicaManager.tryCompleteDelayedFetch(requestKey)
    replicaManager.tryCompleteDelayedProduce(requestKey)
    replicaManager.tryCompleteDelayedDeleteRecords(requestKey)
  }

  //如果ack设置为-1，但是网络延迟，follower副本无法和leader同步。此时进行isr集合缩减
  def maybeShrinkIsr(replicaMaxLagTimeMs: Long) {

    //获得leader副本对应的replica对性
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>

          //检测follower副本的lastCaughtTimeMsUnderlying字段，找出已滞后的follower集合
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs)
          if(outOfSyncReplicas.nonEmpty) {

            //将副本从isr集合中删除，生成新的isr集合
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas
            assert(newInSyncReplicas.nonEmpty)
            info("Shrinking ISR from %s to %s".format(inSyncReplicas.map(_.brokerId).mkString(","),
              newInSyncReplicas.map(_.brokerId).mkString(",")))

            // update ISR in zk and in cache
            //将新isr集合的信息上传到zk中保存，生成新的isr集合
            updateIsr(newInSyncReplicas)
            // we may need to increment high watermark since ISR could be down to 1

            // 标识发生了一次isr集合缩小
            replicaManager.isrShrinkRate.mark()
            //更新leader的hw
            maybeIncrementLeaderHW(leaderReplica)
          } else {
            false
          }

        case None => false // do nothing if no longer leader
      }
    }

    // some delayed operations may be unblocked after HW changed
    //尝试执行延时任务
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
  }

  def getOutOfSyncReplicas(leaderReplica: Replica, maxLagMs: Long): Set[Replica] = {
    /**
     * there are two cases that will be handled here -
     * 1. Stuck followers: If the leo of the replica hasn't been updated for maxLagMs ms,
     *                     the follower is stuck and should be removed from the ISR
     * 2. Slow followers: If the replica has not read up to the leo within the last maxLagMs ms,
     *                    then the follower is lagging and should be removed from the ISR
     * Both these cases are handled by checking the lastCaughtUpTimeMs which represents
     * the last time when the replica was fully caught up. If either of the above conditions
     * is violated, that replica is considered to be out of sync
     *
     **/
    val candidateReplicas = inSyncReplicas - leaderReplica

    val laggingReplicas = candidateReplicas.filter(r => (time.milliseconds - r.lastCaughtUpTimeMs) > maxLagMs)
    if (laggingReplicas.nonEmpty)
      debug("Lagging replicas are %s".format(laggingReplicas.map(_.brokerId).mkString(",")))

    laggingReplicas
  }
  //是有leader副本能处理读写请求，向对应的log中追加消息
  def appendRecordsToLeader(records: MemoryRecords, isFromClient: Boolean, requiredAcks: Int = 0) = {
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {

      //获得leader对应的replica对性
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val log = leaderReplica.log.get

          //获得配置指定的最小isr集合大小
          val minIsr = log.config.minInSyncReplicas
          val inSyncSize = inSyncReplicas.size

          // Avoid writing to leader if there are not enough insync replicas to make it safe

          //如果当前isr集合中副本的数量小于配置的最小数量
          //且生产真要求较高的可用性
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException("Number of insync replicas for partition %s is [%d], below required minimum [%d]"
              .format(topicPartition, inSyncSize, minIsr))
          }

          //写入leader副本对应的log
          val info = log.appendAsLeader(records, leaderEpoch = this.leaderEpoch, isFromClient)

          // probably unblock some follower fetch requests since log end offset has been updated
          //尝试执行delayedFetch
          replicaManager.tryCompleteDelayedFetch(TopicPartitionOperationKey(this.topic, this.partitionId))
          // we may need to increment high watermark since ISR could be down to 1
          (info, maybeIncrementLeaderHW(leaderReplica))

        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d"
            .format(topicPartition, localBrokerId))
      }
    }

    // some delayed operations may be unblocked after HW changed
    //尝试执行延时任务
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()

    info
  }

  /**
   * Update logStartOffset and low watermark if 1) offset <= highWatermark and 2) it is the leader replica.
   * This function can trigger log segment deletion and log rolling.
   *
   * Return low watermark of the partition.
   */
  def deleteRecordsOnLeader(offset: Long): Long = {
    inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          leaderReplica.maybeIncrementLogStartOffset(offset)
          if (!leaderReplica.log.get.config.delete)
            throw new PolicyViolationException("Records of partition %s can not be deleted due to the configured policy".format(topicPartition))
          lowWatermarkIfLeader
        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d"
            .format(topicPartition, localBrokerId))
      }
    }
  }

  /**
    * @param leaderEpoch Requested leader epoch
    * @return The last offset of messages published under this leader epoch.
    */
  def lastOffsetForLeaderEpoch(leaderEpoch: Int): EpochEndOffset = {
    inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          new EpochEndOffset(NONE, leaderReplica.epochs.get.endOffsetFor(leaderEpoch))
        case None =>
          new EpochEndOffset(NOT_LEADER_FOR_PARTITION, UNDEFINED_EPOCH_OFFSET)
      }
    }
  }

  private def updateIsr(newIsr: Set[Replica]) {
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(_.brokerId).toList, zkVersion)
    val (updateSucceeded,newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partitionId,
      newLeaderAndIsr, controllerEpoch, zkVersion)

    if(updateSucceeded) {
      replicaManager.recordIsrChange(topicPartition)
      inSyncReplicas = newIsr
      zkVersion = newVersion
      trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.mkString(","), zkVersion))
    } else {
      replicaManager.failedIsrUpdatesRate.mark()
      info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
    }
  }

  /**
   * remove deleted log metrics
   */
  private def removePartitionMetrics() {
    removeMetric("UnderReplicated", tags)
    removeMetric("InSyncReplicasCount", tags)
    removeMetric("ReplicasCount", tags)
  }

  override def equals(that: Any): Boolean = that match {
    case other: Partition => partitionId == other.partitionId && topic == other.topic
    case _ => false
  }

  override def hashCode: Int =
    31 + topic.hashCode + 17 * partitionId

  override def toString: String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; AssignedReplicas: " + assignedReplicaMap.keys.mkString(","))
    partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
    partitionString.toString
  }
}
