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
package kafka.coordinator.group

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.OffsetAndMetadata
import kafka.log.LogConfig
import kafka.message.ProducerCompressionCodec
import kafka.server._
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch.{NO_PRODUCER_EPOCH, NO_PRODUCER_ID}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, Seq, immutable}
import scala.math.max


/**
 * GroupCoordinator handles general group membership and offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * groups. Groups are assigned to coordinators based on their group names.
 */
// 每个broker上面都会实例化一个GroupCoordinator对象
// 负责处理JoinGroupRequest 和 SysncGroupRequest完成 Consumer group 分配工作
// 通过GroupMetadataManager和内部 offsets topic维护offset信息，即出现消费者宕机也可以找回之前提交的offset
// 记录Consumer group的相关信息，即使broker宕机导致Consumer group有新的GroupCoordinator进行管理，新的GroupCoordinator也可以知道Consumer group中每个消费者负责处理的分区
// 通过心跳消息检测消费者的状态
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupConfig, // 记录了Consumer group中的Comsuemr session过期的最小时长和最大时间
                       val offsetConfig: OffsetConfig, // 记录了offsetMetadata相关的配置项
                       val groupManager: GroupMetadataManager, // 负责管理Consumer group元数据以及其对应offset信息
                       val heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat], // 用于管理DelayedHeartbeat
                       val joinPurgatory: DelayedOperationPurgatory[DelayedJoin], // 用于管理DelayedJoin
                       time: Time) extends Logging {
  import GroupCoordinator._

  type JoinCallback = JoinGroupResult => Unit
  type SyncCallback = (Array[Byte], Errors) => Unit

  this.logIdent = "[GroupCoordinator " + brokerId + "]: "

  private val isActive = new AtomicBoolean(false)

  def offsetsTopicConfigs: Properties = {
    val props = new Properties
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CompressionTypeProp, ProducerCompressionCodec.name)
    props
  }

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup(enableMetadataExpiration: Boolean = true) {
    info("Starting up.")
    if (enableMetadataExpiration)
      groupManager.enableMetadataExpiration()
    isActive.set(true)
    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown() {
    info("Shutting down.")
    isActive.set(false)
    groupManager.shutdown()
    heartbeatPurgatory.shutdown()
    joinPurgatory.shutdown()
    info("Shutdown complete.")
  }

  def handleJoinGroup(groupId: String,
                      memberId: String,
                      clientId: String,
                      clientHost: String,
                      rebalanceTimeoutMs: Int,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(String, Array[Byte])],
                      responseCallback: JoinCallback) {
    // 检测GroupCoordinator是否启动
    if (!isActive.get) {
      responseCallback(joinError(memberId, Errors.COORDINATOR_NOT_AVAILABLE))
    } else if (!validGroupId(groupId)) { // j检测groupid是否合法
      responseCallback(joinError(memberId, Errors.INVALID_GROUP_ID))
    } else if (!isCoordinatorForGroup(groupId)) { // 检测GroupCoordinator是否已经加载此Consumer group对应的Offsets topic分区
      responseCallback(joinError(memberId, Errors.NOT_COORDINATOR))
    } else if (isCoordinatorLoadInProgress(groupId)) {
      responseCallback(joinError(memberId, Errors.COORDINATOR_LOAD_IN_PROGRESS))
    } else if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
               sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) {
      responseCallback(joinError(memberId, Errors.INVALID_SESSION_TIMEOUT))
    } else {
      // only try to create the group if the group is not unknown AND
      // the member id is UNKNOWN, if member is specified but group does not
      // exist we should reject the request
      groupManager.getGroup(groupId) match {
        case None =>
          // 检测memberId是否是合法的
          if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID) {
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
          } else {
            // 创建GroupMetadata
            val group = groupManager.addGroup(new GroupMetadata(groupId))
            // 调用doJoinGroup完成后续功能
            doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
          }

        case Some(group) =>
          doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
      }
    }
  }

  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          clientId: String,
                          clientHost: String,
                          rebalanceTimeoutMs: Int,
                          sessionTimeoutMs: Int,
                          protocolType: String,
                          protocols: List[(String, Array[Byte])],
                          responseCallback: JoinCallback) {
    group synchronized {
      // 检测Member支持的ParitionAssignor
      if (!group.is(Empty) && (!group.protocolType.contains(protocolType) || !group.supportsProtocols(protocols.map(_._1).toSet))) {
        // if the new member does not support the group protocol, reject it
        responseCallback(joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))

        // 检测memberId是否能被识别
      } else if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID && !group.has(memberId)) {
        // if the member trying to register with a un-recognized id, send the response to let
        // it reset its member id and retry
        responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
      } else {
        group.currentState match {
            // 根据consumer group的状态进行分类管理
          case Dead =>
            // if the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; this is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // joining without the specified member id,
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
          case PreparingRebalance =>
            // 未知member申请加入
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType, protocols, group, responseCallback)
            } else {
              // 已知member申请加入
              val member = group.get(memberId)
              updateMemberAndRebalance(group, member, protocols, responseCallback)
            }

          case AwaitingSync =>
            // 未知member申请加入会分生状态切换
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType, protocols, group, responseCallback)
            } else {
              // 已知member申请加入
              val member = group.get(memberId)
              if (member.matches(protocols)) {
                // member is joining with the same metadata (which could be because it failed to
                // receive the initial JoinGroup response), so just return current group information
                // for the current generation.
                // 支持的ParitionAssignor未发生改变，返回groupmetadata信息
                responseCallback(JoinGroupResult(
                  members = if (memberId == group.leaderId) {
                    group.currentMemberMetadata
                  } else {
                    Map.empty
                  },
                  memberId = memberId,
                  generationId = group.generationId,
                  subProtocol = group.protocol,
                  leaderId = group.leaderId,
                  error = Errors.NONE))
              } else {
                // member has changed metadata, so force a rebalance
                // 支持的ParitionAssignor发生改变，需要更新Member信息并发声状态切换
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              }
            }

          case Empty | Stable =>
            // 未知member申请加入会发生状态切换
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              // if the member id is unknown, register the member to the group
              addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType, protocols, group, responseCallback)
            } else {
              // 已知member重新申请加入
              val member = group.get(memberId)

              // group leader支持的ParitionAssignor发生改变，则更新Member信息并发生状态切换
              if (memberId == group.leaderId || !member.matches(protocols)) {
                // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
                // The latter allows the leader to trigger rebalances for changes affecting assignment
                // which do not affect the member metadata (such as topic metadata changes for the consumer)
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              } else {
                // for followers with no actual change to their metadata, just return group information
                // for the current generation which will allow them to issue SyncGroup

                // 支持的ParitionAssignore未发生改变，返回GroupMetadata信息
                responseCallback(JoinGroupResult(
                  members = Map.empty,
                  memberId = memberId,
                  generationId = group.generationId,
                  subProtocol = group.protocol,
                  leaderId = group.leaderId,
                  error = Errors.NONE))
              }
            }
        }

        // 尝试完成相关的DelayedJoin
        if (group.is(PreparingRebalance))
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))
      }
    }
  }

  def handleSyncGroup(groupId: String,
                      generation: Int,
                      memberId: String,
                      groupAssignment: Map[String, Array[Byte]],
                      responseCallback: SyncCallback) {
    if (!isActive.get) {
      responseCallback(Array.empty, Errors.COORDINATOR_NOT_AVAILABLE)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Array.empty, Errors.NOT_COORDINATOR)
    } else {
      groupManager.getGroup(groupId) match {
        case None => responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)
          // 调用doSyncGroup
        case Some(group) => doSyncGroup(group, generation, memberId, groupAssignment, responseCallback)
      }
    }
  }

  private def doSyncGroup(group: GroupMetadata,
                          generationId: Int,
                          memberId: String,
                          groupAssignment: Map[String, Array[Byte]],
                          responseCallback: SyncCallback) {
    group synchronized {
      // 判断member是否合法
      if (!group.has(memberId)) {
        responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)
        // 检测generationId是否合法
      } else if (generationId != group.generationId) {
        responseCallback(Array.empty, Errors.ILLEGAL_GENERATION)
      } else {
        // 分状态处理
        group.currentState match {
          case Empty | Dead =>
            responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)

          case PreparingRebalance =>
            responseCallback(Array.empty, Errors.REBALANCE_IN_PROGRESS)

          case AwaitingSync =>
            // 设置回调函数
            group.get(memberId).awaitingSyncCallback = responseCallback

            // if this is the leader, then we can attempt to persist state and transition to stable
            // 处理GroupLeader发来的SyncGroupRequest
            if (memberId == group.leaderId) {
              info(s"Assignment received from leader for group ${group.groupId} for generation ${group.generationId}")

              // fill any missing members with an empty assignment
              // 将未分配到分区的member对应的分配结果填充为空的byte数组
              val missing = group.allMembers -- groupAssignment.keySet
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

              // 将groupmetadata相关信息形成消息，并写入到对应的offsets topic分区中
              groupManager.storeGroup(group, assignment, (error: Errors) => {
                group synchronized {
                  // another member may have joined the group while we were awaiting this callback,
                  // so we must ensure we are still in the AwaitingSync state and the same generation
                  // when it gets invoked. if we have transitioned to another state, then do nothing
                  if (group.is(AwaitingSync) && generationId == group.generationId) {
                    if (error != Errors.NONE) {
                      // 清空分区的分配结果，发送异常响应
                      resetAndPropagateAssignmentError(group, error)
                      // 切换成PrepareRebalance
                      maybePrepareRebalance(group)
                    } else {
                      // 设置分区的分配结果
                      setAndPropagateAssignment(group, assignment)
                      group.transitionTo(Stable)
                    }
                  }
                }
              })
            }

          case Stable =>
            // if the group is stable, we just return the current assignment
            // 设置分区的分配结果，发送正常的response
            val memberMetadata = group.get(memberId)
            responseCallback(memberMetadata.assignment, Errors.NONE)
            // 心跳相关操作
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))
        }
      }
    }
  }

  def handleLeaveGroup(groupId: String, memberId: String, responseCallback: Errors => Unit) {
    // 检测groupCoordinator是否启动
    if (!isActive.get) {
      responseCallback(Errors.COORDINATOR_NOT_AVAILABLE)
      // groupCoodinator是否管理此consumer group
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Errors.NOT_COORDINATOR)
      // groupCoodinator是否已经加载此comsumer group 对应的offsets topic
    } else if (isCoordinatorLoadInProgress(groupId)) {
      responseCallback(Errors.COORDINATOR_LOAD_IN_PROGRESS)
    } else {
      groupManager.getGroup(groupId) match {
        case None =>
          // if the group is marked as dead, it means some other thread has just removed the group
          // from the coordinator metadata; this is likely that the group has migrated to some other
          // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
          // joining without specified consumer id,
          responseCallback(Errors.UNKNOWN_MEMBER_ID)

        case Some(group) =>
          group synchronized {
            // group处于dead,未知的memberid
            if (group.is(Dead) || !group.has(memberId)) {
              responseCallback(Errors.UNKNOWN_MEMBER_ID)
            } else {
              val member = group.get(memberId)
              // 将对应的membermetadata的isleaving字段设为true,尝试完成相应的delayedheartbeat
              removeHeartbeatForLeavingMember(group, member)
              // 移除响应的membermetadata对象
              onMemberFailure(group, member)
              // 调用回调函数
              responseCallback(Errors.NONE)
            }
          }
      }
    }
  }

  def handleHeartbeat(groupId: String,
                      memberId: String,
                      generationId: Int,
                      responseCallback: Errors => Unit) {
    if (!isActive.get) {
      responseCallback(Errors.COORDINATOR_NOT_AVAILABLE)
      // 检测groupCoodinator是否管理该comsumer group
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Errors.NOT_COORDINATOR)
      // 是否已经加载对应的offsets topic分区
    } else if (isCoordinatorLoadInProgress(groupId)) {
      // the group is still loading, so respond just blindly
      responseCallback(Errors.NONE)
    } else {
      groupManager.getGroup(groupId) match {
          // 检测Groupmetadata是否存在
        case None =>
          responseCallback(Errors.UNKNOWN_MEMBER_ID)

        case Some(group) =>
          group synchronized {
            group.currentState match {
              case Dead =>
                // if the group is marked as dead, it means some other thread has just removed the group
                // from the coordinator metadata; this is likely that the group has migrated to some other
                // coordinator OR the group is in a transient unstable phase. Let the member retry
                // joining without the specified member id,
                responseCallback(Errors.UNKNOWN_MEMBER_ID)

              case Empty =>
                responseCallback(Errors.UNKNOWN_MEMBER_ID)

              case AwaitingSync =>
                // 检测memberid
                if (!group.has(memberId))
                  responseCallback(Errors.UNKNOWN_MEMBER_ID)
                else
                  responseCallback(Errors.REBALANCE_IN_PROGRESS)

              case PreparingRebalance =>
                if (!group.has(memberId)) {
                  responseCallback(Errors.UNKNOWN_MEMBER_ID)
                } else if (generationId != group.generationId) {
                  responseCallback(Errors.ILLEGAL_GENERATION)
                } else {
                  val member = group.get(memberId)
                  // 继续下一步操作
                  completeAndScheduleNextHeartbeatExpiration(group, member)
                  responseCallback(Errors.REBALANCE_IN_PROGRESS)
                }

              case Stable =>
                if (!group.has(memberId)) {
                  responseCallback(Errors.UNKNOWN_MEMBER_ID)
                } else if (generationId != group.generationId) {
                  responseCallback(Errors.ILLEGAL_GENERATION)
                } else {
                  val member = group.get(memberId)
                  completeAndScheduleNextHeartbeatExpiration(group, member)
                  responseCallback(Errors.NONE)
                }
            }
          }
      }
    }
  }

  def handleTxnCommitOffsets(groupId: String,
                             producerId: Long,
                             producerEpoch: Short,
                             offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                             responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    validateGroup(groupId) match {
      case Some(error) => responseCallback(offsetMetadata.mapValues(_ => error))
      case None =>
        val group = groupManager.getGroup(groupId).getOrElse(groupManager.addGroup(new GroupMetadata(groupId)))
        doCommitOffsets(group, NoMemberId, NoGeneration, producerId, producerEpoch, offsetMetadata, responseCallback)
    }
  }

  def handleCommitOffsets(groupId: String,
                          memberId: String,
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Errors] => Unit) {
    validateGroup(groupId) match {
      case Some(error) => responseCallback(offsetMetadata.mapValues(_ => error))
      case None =>
        // 如果对应的GroupMetadata对象不存在且generationid<0 则表示groupcoordinator不维护group的分配结果，只记录offsets信息
        // 消费者手动指定assign() 消费者和分区的关系
        groupManager.getGroup(groupId) match {
          case None =>
            if (generationId < 0) {
              // the group is not relying on Kafka for group management, so allow the commit
              val group = groupManager.addGroup(new GroupMetadata(groupId))
              doCommitOffsets(group, memberId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
                offsetMetadata, responseCallback)
            } else {
              // or this is a request coming from an older generation. either way, reject the commit
              responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION))
            }

          case Some(group) =>
            doCommitOffsets(group, memberId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
              offsetMetadata, responseCallback)
        }
    }
  }

  def handleTxnCompletion(producerId: Long,
                          offsetsPartitions: Iterable[TopicPartition],
                          transactionResult: TransactionResult) {
    require(offsetsPartitions.forall(_.topic == Topic.GROUP_METADATA_TOPIC_NAME))
    val isCommit = transactionResult == TransactionResult.COMMIT
    groupManager.handleTxnCompletion(producerId, offsetsPartitions.map(_.partition).toSet, isCommit)
  }

  private def doCommitOffsets(group: GroupMetadata,
                              memberId: String,
                              generationId: Int,
                              producerId: Long,
                              producerEpoch: Short,
                              offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                              responseCallback: immutable.Map[TopicPartition, Errors] => Unit) {
    group synchronized {
      if (group.is(Dead)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID))
      } else if ((generationId < 0 && group.is(Empty)) || (producerId != NO_PRODUCER_ID)) {
        // the group is only using Kafka to store offsets
        // Also, for transactional offset commits we don't need to validate group membership and the generation.
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback, producerId, producerEpoch)
      } else if (group.is(AwaitingSync)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.REBALANCE_IN_PROGRESS))
      } else if (!group.has(memberId)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID))
      } else if (generationId != group.generationId) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION))
      } else {
        val member = group.get(memberId)
        completeAndScheduleNextHeartbeatExpiration(group, member)
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback)
      }
    }
  }

  def handleFetchOffsets(groupId: String,
                         partitions: Option[Seq[TopicPartition]] = None): (Errors, Map[TopicPartition, OffsetFetchResponse.PartitionData]) = {
    if (!isActive.get)
      (Errors.COORDINATOR_NOT_AVAILABLE, Map())
    else if (!isCoordinatorForGroup(groupId)) { // 检测GroupCoordinator是否是Consumer group的管理者
      debug("Could not fetch offsets for group %s (not group coordinator).".format(groupId))
      (Errors.NOT_COORDINATOR, Map())
    } else if (isCoordinatorLoadInProgress(groupId)) // 检测GroupMetadata是否已经完成加载
      (Errors.COORDINATOR_LOAD_IN_PROGRESS, Map())
    else {
      // return offsets blindly regardless the current group state since the group may be using
      // Kafka commit storage without automatic group management
      // 交给GroupMetadataManager处理
      (Errors.NONE, groupManager.getOffsets(groupId, partitions))
    }
  }

  def handleListGroups(): (Errors, List[GroupOverview]) = {
    if (!isActive.get) {
      (Errors.COORDINATOR_NOT_AVAILABLE, List[GroupOverview]())
    } else {
      val errorCode = if (groupManager.isLoading) Errors.COORDINATOR_LOAD_IN_PROGRESS else Errors.NONE
      (errorCode, groupManager.currentGroups.map(_.overview).toList)
    }
  }

  def handleDescribeGroup(groupId: String): (Errors, GroupSummary) = {
    if (!isActive.get) {
      (Errors.COORDINATOR_NOT_AVAILABLE, GroupCoordinator.EmptyGroup)
    } else if (!isCoordinatorForGroup(groupId)) {
      (Errors.NOT_COORDINATOR, GroupCoordinator.EmptyGroup)
    } else if (isCoordinatorLoadInProgress(groupId)) {
      (Errors.COORDINATOR_LOAD_IN_PROGRESS, GroupCoordinator.EmptyGroup)
    } else {
      groupManager.getGroup(groupId) match {
        case None => (Errors.NONE, GroupCoordinator.DeadGroup)
        case Some(group) =>
          group synchronized {
            (Errors.NONE, group.summary)
          }
      }
    }
  }

  def handleDeletedPartitions(topicPartitions: Seq[TopicPartition]) {
    groupManager.cleanupGroupMetadata(Some(topicPartitions))
  }

  private def validateGroup(groupId: String): Option[Errors] = {
    if (!isActive.get)
      Some(Errors.COORDINATOR_NOT_AVAILABLE)
    else if (!isCoordinatorForGroup(groupId))
      Some(Errors.NOT_COORDINATOR)
    else if (isCoordinatorLoadInProgress(groupId))
      Some(Errors.COORDINATOR_LOAD_IN_PROGRESS)
    else
      None
  }
  // 在groupmetadata被 删除前，将comsumer group状态转换为dead,根据之前的consumer group状态进行相应的清理操作
  private def onGroupUnloaded(group: GroupMetadata) {
    group synchronized {
      info(s"Unloading group metadata for ${group.groupId} with generation ${group.generationId}")
      val previousState = group.currentState

      // 切换到dead状态
      group.transitionTo(Dead)
      // 根据consumer group之前的状态进行处理
      previousState match {
        case Empty | Dead =>
        case PreparingRebalance =>
          // 调用全部的member的awaitingjoinCallbakc函数
          for (member <- group.allMemberMetadata) {
            if (member.awaitingJoinCallback != null) {
              member.awaitingJoinCallback(joinError(member.memberId, Errors.NOT_COORDINATOR))
              member.awaitingJoinCallback = null
            }
          }
          // awaitingJoinCallback 的变化，可能导致delayedJoin满足条件，常熟
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))

        case Stable | AwaitingSync =>
          for (member <- group.allMemberMetadata) {
            if (member.awaitingSyncCallback != null) {
              member.awaitingSyncCallback(Array.empty[Byte], Errors.NOT_COORDINATOR)
              member.awaitingSyncCallback = null
            }
            // 尝试执行delayedHeartbeat
            heartbeatPurgatory.checkAndComplete(MemberKey(member.groupId, member.memberId))
          }
      }
    }
  }

  private def onGroupLoaded(group: GroupMetadata) {
    group synchronized {
      info(s"Loading group metadata for ${group.groupId} with generation ${group.generationId}")
      assert(group.is(Stable) || group.is(Empty))
      // 更新所有member的心跳操作
      group.allMemberMetadata.foreach(completeAndScheduleNextHeartbeatExpiration(group, _))
    }
  }

  def handleGroupImmigration(offsetTopicPartitionId: Int) {
    groupManager.loadGroupsForPartition(offsetTopicPartitionId, onGroupLoaded)
  }

  def handleGroupEmigration(offsetTopicPartitionId: Int) {
    groupManager.removeGroupsForPartition(offsetTopicPartitionId, onGroupUnloaded)
  }

  private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]) {
    assert(group.is(AwaitingSync))
    group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))
    propagateAssignment(group, Errors.NONE)
  }

  // 对于AwaitingSync状态的consumer group来说。有的group follower已经发送了syncGroupRequest
  // GroupCooridnator在等待Group leader通过SyncGroupRequest将分区的分配结果发送过来
  private def resetAndPropagateAssignmentError(group: GroupMetadata, error: Errors) {
    assert(group.is(AwaitingSync))
    //清空所有mebermetadata的assignment字段
    group.allMemberMetadata.foreach(_.assignment = Array.empty[Byte])
    propagateAssignment(group, error)
  }

  private def propagateAssignment(group: GroupMetadata, error: Errors) {
    for (member <- group.allMemberMetadata) {
      if (member.awaitingSyncCallback != null) {
        // d调用awairingsynccallback函数，向对应的consumer发送syncgroupResponse
        member.awaitingSyncCallback(member.assignment, error)
        // 清空回调函数
        member.awaitingSyncCallback = null

        // reset the session timeout for members after propagating the member's assignment.
        // This is because if any member's session expired while we were still awaiting either
        // the leader sync group or the storage callback, its expiration will be ignored and no
        // future heartbeat expectations will not be scheduled.
        // 开启等待下次心跳的延迟任务
        completeAndScheduleNextHeartbeatExpiration(group, member)
      }
    }
  }

  private def validGroupId(groupId: String): Boolean = {
    groupId != null && !groupId.isEmpty
  }

  private def joinError(memberId: String, error: Errors): JoinGroupResult = {
    JoinGroupResult(
      members = Map.empty,
      memberId = memberId,
      generationId = 0,
      subProtocol = GroupCoordinator.NoProtocol,
      leaderId = GroupCoordinator.NoLeader,
      error = error)
  }

  /**
   * Complete existing DelayedHeartbeats for the given member and schedule the next one
   */
  // 更新收到此member心跳的时间戳，尝试执行其对应的delayedheartbeat
  private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, member: MemberMetadata) {
    // complete current heartbeat expectation
    // 更新心跳时间
    member.latestHeartbeat = time.milliseconds()
    // 获取delayedheartbeat的key
    val memberKey = MemberKey(member.groupId, member.memberId)
    // 尝试完成之前添加的delayedheartbeat
    heartbeatPurgatory.checkAndComplete(memberKey)

    // reschedule the next heartbeat expiration deadline
    // 计算下一次的heartbeant的超时时间
    val newHeartbeatDeadline = member.latestHeartbeat + member.sessionTimeoutMs
    // 创建新的delayedheartbeat对象，并添加到heartbeatPurgatory
    val delayedHeartbeat = new DelayedHeartbeat(this, group, member, newHeartbeatDeadline, member.sessionTimeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
  }

  private def removeHeartbeatForLeavingMember(group: GroupMetadata, member: MemberMetadata) {
    member.isLeaving = true
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)
  }
  // 添加member
  private def addMemberAndRebalance(rebalanceTimeoutMs: Int,
                                    sessionTimeoutMs: Int,
                                    clientId: String,
                                    clientHost: String,
                                    protocolType: String,
                                    protocols: List[(String, Array[Byte])],
                                    group: GroupMetadata,
                                    callback: JoinCallback) = {
    // 创建memberid
    val memberId = clientId + "-" + group.generateMemberIdSuffix
    // 创建新的MemberMetadata对象
    val member = new MemberMetadata(memberId, group.groupId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, protocols)

    // 设置回调函数。在kafkaApis.handleJoinGroupRequest方法中定义的sendResponseCallBack
    member.awaitingJoinCallback = callback
    // update the newMemberAdded flag to indicate that the join group can be further delayed
    if (group.is(PreparingRebalance) && group.generationId == 0)
      group.newMemberAdded = true

    // 添加到GroupMetadata中保存
    group.add(member)

    // 尝试进行状态切换
    maybePrepareRebalance(group)
    member
  }

  // 更新member
  private def updateMemberAndRebalance(group: GroupMetadata,
                                       member: MemberMetadata,
                                       protocols: List[(String, Array[Byte])],
                                       callback: JoinCallback) {
    // 更新MemberMetadata支持的协议和awaitingJoinCallBack函数
    member.supportedProtocols = protocols
    member.awaitingJoinCallback = callback
    // 尝试进行状态的切换
    maybePrepareRebalance(group)
  }

  private def maybePrepareRebalance(group: GroupMetadata) {
    group synchronized {
      if (group.canRebalance)
        prepareRebalance(group)
    }
  }

  private def prepareRebalance(group: GroupMetadata) {
    // if any members are awaiting sync, cancel their request and have them rejoin
    // 处于awaitingSync状态，首先要充值MemberMetadata.assignment字段
    // 调用awaitingSyncCallback想消费者返回错误码
    if (group.is(AwaitingSync))
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

    val delayedRebalance = if (group.is(Empty))
      new InitialDelayedJoin(this,
        joinPurgatory,
        group,
        groupConfig.groupInitialRebalanceDelayMs,
        groupConfig.groupInitialRebalanceDelayMs,
        max(group.rebalanceTimeoutMs - groupConfig.groupInitialRebalanceDelayMs, 0))
    else
      new DelayedJoin(this, group, group.rebalanceTimeoutMs)

    // 将Comsumer group状转换成PreparingRebalance状态，表示要执行Rebalance操作
    group.transitionTo(PreparingRebalance)

    info(s"Preparing to rebalance group ${group.groupId} with old generation ${group.generationId} " +
      s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

    val groupKey = GroupKey(group.groupId)
    // 尝试立即完成DelayedJosin 否则将DelayedFetch添加到joinPurgatory中
    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }

  private def onMemberFailure(group: GroupMetadata, member: MemberMetadata) {
    debug(s"Member ${member.memberId} in group ${group.groupId} has failed")
    // 将对应的member从groupmetadata中删除
    group.remove(member.memberId)
    group.currentState match {
      case Dead | Empty =>
        // 之前的分区可能已经失效了，将groupmetadata切换成preparingRebalance状态
      case Stable | AwaitingSync => maybePrepareRebalance(group)
        // groupmetadata中的member减少，可能满足delayedjoin的执行条件
      case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean) = {
    group synchronized {
      // 判断已知Member是否已经申请加入
      if (group.notYetRejoinedMembers.isEmpty)
        forceComplete()
      else false
    }
  }

  def onExpireJoin() {
    // TODO: add metrics for restabilize timeouts
  }
  // 当已知Member都已经申请重新加入或Delayed到期
  def onCompleteJoin(group: GroupMetadata) {
    group synchronized {
      // remove any members who haven't joined the group yet
      group.notYetRejoinedMembers.foreach { failedMember =>
        // 移除未加入的已知Member
        group.remove(failedMember.memberId)
        // TODO: cut the socket connection to the client
      }

      if (!group.is(Dead)) {
        group.initNextGeneration()
        // 如果groupmetadata中已经没有member，则将groupmetadata切换成dead状态
        if (group.is(Empty)) {
          info(s"Group ${group.groupId} with generation ${group.generationId} is now empty " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

          groupManager.storeGroup(group, Map.empty, error => {
            if (error != Errors.NONE) {
              // we failed to write the empty group metadata. If the broker fails before another rebalance,
              // the previous generation written to the log will become active again (and most likely timeout).
              // This should be safe since there are no active members in an empty generation, so we just warn.
              warn(s"Failed to write empty metadata for group ${group.groupId}: ${error.message}")
            }
          })
        } else {
          info(s"Stabilized group ${group.groupId} generation ${group.generationId} " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

          // trigger the awaiting join group response callback for all the members after rebalancing
          // 向groupmetadata中所有的member发送joingroupresponse
          for (member <- group.allMemberMetadata) {
            assert(member.awaitingJoinCallback != null)
            val joinResult = JoinGroupResult(
              members = if (member.memberId == group.leaderId) {
                group.currentMemberMetadata
              } else {
                Map.empty
              },
              memberId = member.memberId,
              generationId = group.generationId,
              subProtocol = group.protocol,
              leaderId = group.leaderId,
              error = Errors.NONE)

            member.awaitingJoinCallback(joinResult)
            member.awaitingJoinCallback = null
            // 心跳操作
            completeAndScheduleNextHeartbeatExpiration(group, member)
          }
        }
      }
    }
  }

  def tryCompleteHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long, forceComplete: () => Boolean) = {
    group synchronized {
      if (shouldKeepMemberAlive(member, heartbeatDeadline)  // 检测条件1-3
        || member.isLeaving) // 消费者已经离开了consumer group
        forceComplete()
      else false
    }
  }

  // 将对应的member从groupmetadata中删除
  def onExpireHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long) {
    group synchronized {
      // 检测member是否下线
      if (!shouldKeepMemberAlive(member, heartbeatDeadline))
        // 下线后的相关处理
        onMemberFailure(group, member)
    }
  }

  def onCompleteHeartbeat() {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = groupManager.partitionFor(group)

  private def shouldKeepMemberAlive(member: MemberMetadata, heartbeatDeadline: Long) =
    member.awaitingJoinCallback != null || // 消费者正在等待JoinGroupResponse
      member.awaitingSyncCallback != null || // 消费者正在等待SyncgroupResponse
      member.latestHeartbeat + member.sessionTimeoutMs > heartbeatDeadline //  最后一次收到心跳信息的时间与heartbeatDeadline的差距大于sessionTimeoutMs

  private def isCoordinatorForGroup(groupId: String) = groupManager.isGroupLocal(groupId)

  private def isCoordinatorLoadInProgress(groupId: String) = groupManager.isGroupLoading(groupId)
}

object GroupCoordinator {

  val NoState = ""
  val NoProtocolType = ""
  val NoProtocol = ""
  val NoLeader = ""
  val NoGeneration = -1
  val NoMemberId = ""
  val NoMembers = List[MemberSummary]()
  val EmptyGroup = GroupSummary(NoState, NoProtocolType, NoProtocol, NoMembers)
  val DeadGroup = GroupSummary(Dead.toString, NoProtocolType, NoProtocol, NoMembers)

  def apply(config: KafkaConfig,
            zkUtils: ZkUtils,
            replicaManager: ReplicaManager,
            time: Time): GroupCoordinator = {
    val heartbeatPurgatory = DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId)
    val joinPurgatory = DelayedOperationPurgatory[DelayedJoin]("Rebalance", config.brokerId)
    apply(config, zkUtils, replicaManager, heartbeatPurgatory, joinPurgatory, time)
  }

  private[group] def offsetConfig(config: KafkaConfig) = OffsetConfig(
    maxMetadataSize = config.offsetMetadataMaxSize,
    loadBufferSize = config.offsetsLoadBufferSize,
    offsetsRetentionMs = config.offsetsRetentionMinutes * 60L * 1000L,
    offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
    offsetsTopicNumPartitions = config.offsetsTopicPartitions,
    offsetsTopicSegmentBytes = config.offsetsTopicSegmentBytes,
    offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
    offsetsTopicCompressionCodec = config.offsetsTopicCompressionCodec,
    offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
    offsetCommitRequiredAcks = config.offsetCommitRequiredAcks
  )

  def apply(config: KafkaConfig,
            zkUtils: ZkUtils,
            replicaManager: ReplicaManager,
            heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
            joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
            time: Time): GroupCoordinator = {
    val offsetConfig = this.offsetConfig(config)
    val groupConfig = GroupConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
      groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs,
      groupInitialRebalanceDelayMs = config.groupInitialRebalanceDelay)

    val groupMetadataManager = new GroupMetadataManager(config.brokerId, config.interBrokerProtocolVersion,
      offsetConfig, replicaManager, zkUtils, time)
    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, groupMetadataManager, heartbeatPurgatory, joinPurgatory, time)
  }

}

case class GroupConfig(groupMinSessionTimeoutMs: Int,
                       groupMaxSessionTimeoutMs: Int,
                       groupInitialRebalanceDelayMs: Int)

case class JoinGroupResult(members: Map[String, Array[Byte]],
                           memberId: String,
                           generationId: Int,
                           subProtocol: String,
                           leaderId: String,
                           error: Errors)
