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

import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.common.{LeaderElectionNotNeededException, NoReplicaOnlineException, StateChangeFailedException, TopicAndPartition}
import kafka.log.LogConfig
import kafka.server.{ConfigType, KafkaConfig}
import kafka.utils.Logging
//leader选角，确定isr集合
trait PartitionLeaderSelector {

  /**
   * @param topicAndPartition          The topic and partition whose leader needs to be elected 当前分区
   * @param currentLeaderAndIsr        The current leader and isr of input partition read from zookeeper leader和isr信息
   * @throws NoReplicaOnlineException If no replica in the assigned replicas list is alive
   * @return The leader and isr request, with the newly selected leader and isr, and the set of replicas to receive
   * the LeaderAndIsrRequest.
   */
  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int])

}

/**
 * Select the new leader, new isr and receiving replicas (for the LeaderAndIsrRequest):
 * 1. If at least one broker from the isr is alive, it picks a broker from the live isr as the new leader and the live
 *    isr as the new isr.
 * 2. Else, if unclean leader election for the topic is disabled, it throws a NoReplicaOnlineException.
 * 3. Else, it picks some alive broker from the assigned replica list as the new leader and the new isr.
 * 4. If no broker in the assigned replica list is alive, it throws a NoReplicaOnlineException
 * Replicas to receive LeaderAndIsr request = live assigned replicas
 * Once the leader is successfully registered in zookeeper, it updates the allLeaders cache
 */
//如果在isr集合中存在至少一个可用的副本，在从isr集合中选举新的leader副本
//如果isr集合中没有可用的副本，切unclean leader election配置被禁用，则抛出异常
//如果unclean leader election被开启，则从ar集合中选择新的leader副本和isr集合
//如果ar集合中没有可用的副本，抛出异常
class OfflinePartitionLeaderSelector(controllerContext: ControllerContext, config: KafkaConfig)
  extends PartitionLeaderSelector with Logging {

  logIdent = "[OfflinePartitionLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    //获取分区的ar集合
    controllerContext.partitionReplicaAssignment.get(topicAndPartition) match {
      case Some(assignedReplicas) =>
        //获取ar集合中可用的副本
        val liveAssignedReplicas = assignedReplicas.filter(r => controllerContext.liveBrokerIds.contains(r))
        //获取isr集合中可用的副本
        val liveBrokersInIsr = currentLeaderAndIsr.isr.filter(r => controllerContext.liveBrokerIds.contains(r))

        //检测当前isr集合中是否有可用的副本
        val newLeaderAndIsr =
          if (liveBrokersInIsr.isEmpty) {
            // Prior to electing an unclean (i.e. non-ISR) leader, ensure that doing so is not disallowed by the configuration
            // for unclean leader election.
            if (!LogConfig.fromProps(config.originals, AdminUtils.fetchEntityConfig(controllerContext.zkUtils,
              ConfigType.Topic, topicAndPartition.topic)).uncleanLeaderElectionEnable) {
              throw new NoReplicaOnlineException(
                s"No broker in ISR for partition $topicAndPartition is alive. Live brokers are: [${controllerContext.liveBrokerIds}], " +
                  s"ISR brokers are: [${currentLeaderAndIsr.isr.mkString(",")}]"
              )
            }
            debug(s"No broker in ISR is alive for $topicAndPartition. Pick the leader from the alive assigned " +
              s"replicas: ${liveAssignedReplicas.mkString(",")}")

            if (liveAssignedReplicas.isEmpty) {
              throw new NoReplicaOnlineException(s"No replica for partition $topicAndPartition is alive. Live " +
                s"brokers are: [${controllerContext.liveBrokerIds}]. Assigned replicas are: [$assignedReplicas].")
            } else {
              controllerContext.stats.uncleanLeaderElectionRate.mark()
              val newLeader = liveAssignedReplicas.head
              warn(s"No broker in ISR is alive for $topicAndPartition. Elect leader $newLeader from live " +
                s"brokers ${liveAssignedReplicas.mkString(",")}. There's potential data loss.")
              currentLeaderAndIsr.newLeaderAndIsr(newLeader, List(newLeader))
            }
          } else {
            //从当前isr集合中选择leader副本已经isr集合，构造新leader返回
            val liveReplicasInIsr = liveAssignedReplicas.filter(r => liveBrokersInIsr.contains(r))
            val newLeader = liveReplicasInIsr.head
            debug(s"Some broker in ISR is alive for $topicAndPartition. Select $newLeader from ISR " +
              s"${liveBrokersInIsr.mkString(",")} to be the leader.")
            currentLeaderAndIsr.newLeaderAndIsr(newLeader, liveBrokersInIsr)
          }
        info(s"Selected new leader and ISR $newLeaderAndIsr for offline partition $topicAndPartition")
        //向ar集合中所有可用的副本发送LeaderAndIsrRequest
        (newLeaderAndIsr, liveAssignedReplicas)
      case None =>
        throw new NoReplicaOnlineException(s"Partition $topicAndPartition doesn't have replicas assigned to it")
    }
  }
}

/**
 * New leader = a live in-sync reassigned replica
 * New isr = current isr
 * Replicas to receive LeaderAndIsr request = reassigned replicas
 */
//副本的重新分配
//选取新leader副本必须在新制定的ar集合中且同时在当前isr集合中。当前isr集合为新isr集合，接受leaderAndIsrRequest副本是新制定的ar集合中的副本
class ReassignedPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

  logIdent = "[ReassignedPartitionLeaderSelector]: "

  /**
   * The reassigned replicas are already in the ISR when selectLeader is called.
   */
  def selectLeader(topicAndPartition: TopicAndPartition,
                   currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    val reassignedInSyncReplicas = controllerContext.partitionsBeingReassigned(topicAndPartition).newReplicas
    val newLeaderOpt = reassignedInSyncReplicas.find { r =>
      controllerContext.liveBrokerIds.contains(r) && currentLeaderAndIsr.isr.contains(r)
    }
    newLeaderOpt match {
      case Some(newLeader) => (currentLeaderAndIsr.newLeader(newLeader), reassignedInSyncReplicas)
      case None =>
        val errorMessage = if (reassignedInSyncReplicas.isEmpty) {
          s"List of reassigned replicas for partition $topicAndPartition is empty. Current leader and ISR: " +
            s"[$currentLeaderAndIsr]"
        } else {
          s"None of the reassigned replicas for partition $topicAndPartition are in-sync with the leader. " +
            s"Current leader and ISR: [$currentLeaderAndIsr]"
        }
        throw new NoReplicaOnlineException(errorMessage)
    }
  }
}

/**
 * New leader = preferred (first assigned) replica (if in isr and alive);
 * New isr = current isr;
 * Replicas to receive LeaderAndIsr request = assigned replicas
 */
//如果优先副本可用且在isr集合中，则选取其为leader副本
//当前的isr集合为新的isr集合，并向ar集合中所有可用的副本发送LeaderAndisrRequest
class PreferredReplicaPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

  logIdent = "[PreferredReplicaPartitionLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition,
                   currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    val preferredReplica = assignedReplicas.head
    // check if preferred replica is the current leader
    val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
    if (currentLeader == preferredReplica) {
      throw new LeaderElectionNotNeededException("Preferred replica %d is already the current leader for partition %s"
                                                   .format(preferredReplica, topicAndPartition))
    } else {
      info("Current leader %d for partition %s is not the preferred replica.".format(currentLeader, topicAndPartition) +
        " Triggering preferred replica leader election")
      // check if preferred replica is not the current leader and is alive and in the isr
      if (controllerContext.liveBrokerIds.contains(preferredReplica) && currentLeaderAndIsr.isr.contains(preferredReplica)) {
        val newLeaderAndIsr = currentLeaderAndIsr.newLeader(preferredReplica)
        (newLeaderAndIsr, assignedReplicas)
      } else {
        throw new StateChangeFailedException(s"Preferred replica $preferredReplica for partition $topicAndPartition " +
          s"is either not alive or not in the isr. Current leader and ISR: [$currentLeaderAndIsr]")
      }
    }
  }
}

/**
 * New leader = replica in isr that's not being shutdown;
 * New isr = current isr - shutdown replica;
 * Replicas to receive LeaderAndIsr request = live assigned replicas
 */
class ControlledShutdownLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

  logIdent = "[ControlledShutdownLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition,
                   currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    val currentIsr = currentLeaderAndIsr.isr
    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
    val liveAssignedReplicas = assignedReplicas.filter(r => liveOrShuttingDownBrokerIds.contains(r))

    val newIsr = currentIsr.filter(brokerId => !controllerContext.shuttingDownBrokerIds.contains(brokerId))
    liveAssignedReplicas.find(newIsr.contains) match {
      case Some(newLeader) =>
        debug(s"Partition $topicAndPartition : current leader = ${currentLeaderAndIsr.leader}, new leader = $newLeader")
        val newLeaderAndIsr = currentLeaderAndIsr.newLeaderAndIsr(newLeader, newIsr)
        (newLeaderAndIsr, liveAssignedReplicas)
      case None =>
        throw new StateChangeFailedException(s"No other replicas in ISR ${currentIsr.mkString(",")} for $topicAndPartition " +
          s"besides shutting down brokers ${controllerContext.shuttingDownBrokerIds.mkString(",")}")
    }
  }
}

/**
 * Essentially does nothing. Returns the current leader and ISR, and the current
 * set of replicas assigned to a given topic/partition.
 */
class NoOpLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

  logIdent = "[NoOpLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition,
                   currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    warn("I should never have been asked to perform leader election, returning the current LeaderAndIsr and replica assignment.")
    (currentLeaderAndIsr, controllerContext.partitionReplicaAssignment(topicAndPartition))
  }
}
