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

package kafka.server


import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Pool
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import scala.collection._

case class ProducePartitionStatus(requiredOffset: Long, responseStatus: PartitionResponse) {
  @volatile var acksPending = false

  override def toString = "[acksPending: %b, error: %d, startOffset: %d, requiredOffset: %d]"
    .format(acksPending, responseStatus.error.code, responseStatus.baseOffset, requiredOffset)
}

/**
 * The produce metadata maintained by the delayed produce operation
 */
case class ProduceMetadata(produceRequiredAcks: Short,//ack
                           produceStatus: Map[TopicPartition, ProducePartitionStatus]) {

  override def toString = "[requiredAcks: %d, partitionStatus: %s]"
    .format(produceReq uiredAcks, produceStatus)
}

/**
 * A delayed produce operation that can be created by the replica manager and watched
 * in the produce operation purgatory
 */
class DelayedProduce(delayMs: Long,//延迟时长
                     produceMetadata: ProduceMetadata,//为一个ProduceRequest中所有的相关分区记录了一些追加详细后的返回结果，用于判断DelayedProduce是否满足执行条件
                     replicaManager: ReplicaManager,//此DelayedProduce关联的ReplicaManager对象
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit,//任务满足条件或者到期执行
                     lockOpt: Option[Object] = None)
  extends DelayedOperation(delayMs) {

  val lock = lockOpt.getOrElse(this)

  // first update the acks pending variable according to the error code
  //根据写入的消息返回的结果，设置ProducePartitionStatus的acksPending字段和responseStatus字段的值
  produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
    //对应分区写入成功，则等待isr集合中的副本同步
    //出现异常，不用等待，
    if (status.responseStatus.error == Errors.NONE) {
      // Timeout error state will be cleared when required acks are received
      status.acksPending = true
      //预设错误吗，如果isr集合中的副本在此请求超时之外顺利完成了同步，会清楚此错误码
      status.responseStatus.error = Errors.REQUEST_TIMED_OUT
    } else {
      //如果追加日志已抛出日常，则不需要等待
      status.acksPending = false
    }

    trace("Initial partition status for %s is %s".format(topicPartition, status))
  }

  override def safeTryComplete(): Boolean = lock synchronized {
    tryComplete()
  }


  /**
   * The delayed produce operation can be completed if every partition
   * it produces to is satisfied by one of the following:
   *
   * Case A: This broker is no longer the leader: set an error in response
   * Case B: This broker is the leader:
   *   B.1 - If there was a local error thrown while checking if at least requiredAcks
   *         replicas have caught up to this operation: set an error in response
   *   B.2 - Otherwise, set the response with no error.
   */
  //检测是否有满足DelayedProduce的执行条件
  //满足下列任意条件
  //分区leader副本迁移，
  //isr集合中所有副本都完成同步，改分区的leader副本Hw大于ProduceStatus,requiredOffset
  //出现异常，则更新分区对应的ProducePartitionStatus中记录的错误码
  override def tryComplete(): Boolean = {
    // check for each partition if it still has pending acks
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      trace(s"Checking produce satisfaction for $topicPartition, current status $status")
      // skip those partitions that have already been satisfied
      //acksPending 标示是否正在等待ISR集合中其他副本与Leader副本同步requiredOffset之前消息得同步
      if (status.acksPending) {
        val (hasEnough, error) = replicaManager.getPartition(topicPartition) match {
          case Some(partition) =>
            //获得partition
            partition.checkEnoughReplicasReachOffset(status.requiredOffset)
          case None =>
            // Case A找不到分区的Leader
            (false, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        }
        // Case B.1 || B.2 出现异常或者分区leader副本的hw大于对应的requiredOffset
        if (error != Errors.NONE || hasEnough) {
          status.acksPending = false
          status.responseStatus.error = error
        }
      }
    }

    // check if every partition has satisfied at least one of case A or B
    //检查全部的分区是否都已经符合DelayedProduce的执行条件
    if (!produceMetadata.produceStatus.values.exists(_.acksPending))
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      if (status.acksPending) {
        DelayedProduceMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
   * Upon completion, return the current response status along with the error code per partition
   */
  //执行真正的逻辑
  override def onComplete() {
    //根据ProdueMetadata记录的相关信息，为每个Paritition产生相应状态
    val responseStatus = produceMetadata.produceStatus.mapValues(status => status.responseStatus)
    //向RequestChannels中对应的responseQueue中添加ProduceResponse，之后Process线程将其发送个客户端
    responseCallback(responseStatus)
  }
}

object DelayedProduceMetrics extends KafkaMetricsGroup {

  private val aggregateExpirationMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

  private val partitionExpirationMeterFactory = (key: TopicPartition) =>
    newMeter("ExpiresPerSec",
             "requests",
             TimeUnit.SECONDS,
             tags = Map("topic" -> key.topic, "partition" -> key.partition.toString))
  private val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

  def recordExpiration(partition: TopicPartition) {
    aggregateExpirationMeter.mark()
    partitionExpirationMeters.getAndMaybePut(partition).mark()
  }
}

