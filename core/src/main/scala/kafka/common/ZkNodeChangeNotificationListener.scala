/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.common

import java.util.concurrent.atomic.AtomicBoolean

import kafka.utils.{Logging, ZkUtils}
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.I0Itec.zkclient.exception.ZkInterruptedException
import org.I0Itec.zkclient.{IZkChildListener, IZkStateListener}
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._

/**
 * Handle the notificationMessage.
 */
trait NotificationHandler {
  def processNotification(notificationMessage: String)
}

/**
 * A listener that subscribes to seqNodeRoot for any child changes where all children are assumed to be sequence node
 * with seqNodePrefix. When a child is added under seqNodeRoot this class gets notified, it looks at lastExecutedChange
 * number to avoid duplicate processing and if it finds an unprocessed child, it reads its data and calls supplied
 * notificationHandler's processNotification() method with the child's data as argument. As part of processing these changes it also
 * purges any children with currentTime - createTime > changeExpirationMs.
 *
 * The caller/user of this class should ensure that they use zkClient.subscribeStateChanges and call processAllNotifications
 * method of this class from ZkStateChangeListener's handleNewSession() method. This is necessary to ensure that if zk session
 * is terminated and reestablished any missed notification will be processed immediately.
 * @param zkUtils
 * @param seqNodeRoot
 * @param seqNodePrefix
 * @param notificationHandler
 * @param changeExpirationMs
 * @param time
 */
// 会在zk 的 kafka-acl-changes节点上注册NodeChangeListener用于监听其子节点变化
class ZkNodeChangeNotificationListener(private val zkUtils: ZkUtils,
                                       private val seqNodeRoot: String, // 指定监听的路径
                                       private val seqNodePrefix: String, // 持久顺序节点的前置
                                       private val notificationHandler: NotificationHandler, // 监听到seqNodeRoot路径下子节点集合发生变化时
                                       private val changeExpirationMs: Long = 15 * 60 * 1000, // 如果顺序节点创建后，超过changeExpirationMs指定时间，则认为可以删除
                                       private val time: Time = Time.SYSTEM) extends Logging {
  private var lastExecutedChange = -1L
  private val isClosed = new AtomicBoolean(false)

  /**
   * create seqNodeRoot and begin watching for any new children nodes.
   */
  def init() {
    // 确保 /kafka-acl-changes 节点存在
    zkUtils.makeSurePersistentPathExists(seqNodeRoot)
    // 监听 kafka-acl-changes子节点变化
    zkUtils.zkClient.subscribeChildChanges(seqNodeRoot, NodeChangeListener)
    // 注册ZkStateChangeListener监听zk连接状态
    zkUtils.zkClient.subscribeStateChanges(ZkStateChangeListener)
    // 处理 /kafka-acl-changes的子节点
    processAllNotifications()
  }

  def close() = {
    isClosed.set(true)
  }

  /**
   * Process all changes
   */
  def processAllNotifications() {
    val changes = zkUtils.zkClient.getChildren(seqNodeRoot)
    processNotifications(changes.asScala.sorted)
  }

  /**
   * Process the given list of notifications
   */
  private def processNotifications(notifications: Seq[String]) {
    if (notifications.nonEmpty) {
      info(s"Processing notification(s) to $seqNodeRoot")
      try {
        val now = time.milliseconds
        // 遍历子节点
        for (notification <- notifications) {
          // 获取子节点集合
          val changeId = changeNumber(notification)
          // 检查子节点是否已经被处理过
          if (changeId > lastExecutedChange) {
            val changeZnode = seqNodeRoot + "/" + notification
            // 读取状态信息和其中记录的数据
            val (data, _) = zkUtils.readDataMaybeNull(changeZnode)
            // 更新aclCache集合
            data.map(notificationHandler.processNotification(_)).getOrElse {
              logger.warn(s"read null data from $changeZnode when processing notification $notification")
            }
          }
          lastExecutedChange = changeId
        }
        // 删除过期节点
        purgeObsoleteNotifications(now, notifications)
      } catch {
        case e: ZkInterruptedException =>
          if (!isClosed.get)
            throw e
      }
    }
  }

  /**
   * Purges expired notifications.
   *
   * @param now
   * @param notifications
   */
  private def purgeObsoleteNotifications(now: Long, notifications: Seq[String]) {
    for (notification <- notifications.sorted) {
      val notificationNode = seqNodeRoot + "/" + notification
      // 读取节点状态信息和其中记录的数据
      val (data, stat) = zkUtils.readDataMaybeNull(notificationNode)
      if (data.isDefined) {
        // 检测节点是否过期
        if (now - stat.getCtime > changeExpirationMs) {
          debug(s"Purging change notification $notificationNode")
          // 删除节点
          zkUtils.deletePath(notificationNode)
        }
      }
    }
  }

  /* get the change number from a change notification znode */
  private def changeNumber(name: String): Long = name.substring(seqNodePrefix.length).toLong

  /**
   * A listener that gets invoked when a node is created to notify changes.
   */
  object NodeChangeListener extends IZkChildListener {
    override def handleChildChange(path: String, notifications: java.util.List[String]) {
      try {
        import scala.collection.JavaConverters._
        if (notifications != null)
          processNotifications(notifications.asScala.sorted)
      } catch {
        case e: Exception => error(s"Error processing notification change for path = $path and notification= $notifications :", e)
      }
    }
  }

  object ZkStateChangeListener extends IZkStateListener {

    override def handleNewSession() {
      processAllNotifications
    }

    override def handleSessionEstablishmentError(error: Throwable) {
      fatal("Could not establish session with zookeeper", error)
    }

    override def handleStateChanged(state: KeeperState) {
      debug(s"New zookeeper state: ${state}")
    }
  }

}

