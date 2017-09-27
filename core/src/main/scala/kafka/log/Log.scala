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

package kafka.log

import java.io.{File, IOException}
import java.nio.file.Files
import java.text.NumberFormat
import java.util.concurrent.atomic._
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap, TimeUnit}

import kafka.api.KAFKA_0_10_0_IV0
import kafka.common._
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{BrokerTopicStats, FetchDataInfo, LogOffsetMetadata}
import kafka.utils._
import org.apache.kafka.common.errors.{CorruptRecordException, OffsetOutOfRangeException, RecordBatchTooLargeException, RecordTooLargeException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.{IsolationLevel, ListOffsetRequest}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, mutable}
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.utils.{Time, Utils}
import kafka.message.{BrokerCompressionCodec, CompressionCodec, NoCompressionCodec}
import kafka.server.checkpoints.{LeaderEpochCheckpointFile, LeaderEpochFile}
import kafka.server.epoch.{LeaderEpochCache, LeaderEpochFileCache}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import java.util.Map.{Entry => JEntry}
import java.lang.{Long => JLong}
import java.util.regex.Pattern

object LogAppendInfo {
  val UnknownLogAppendInfo = LogAppendInfo(-1, -1, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP,
    NoCompressionCodec, NoCompressionCodec, -1, -1, offsetsMonotonic = false)
}

/**
 * Struct to hold various quantities we compute about each message set before appending to the log
 *
 * @param firstOffset The first offset in the message set unless the message format is less than V2 and we are appending
 *                    to the follower. In that case, this will be the last offset for performance reasons.
 * @param lastOffset The last offset in the message set
 * @param maxTimestamp The maximum timestamp of the message set.
 * @param offsetOfMaxTimestamp The offset of the message with the maximum timestamp.
 * @param logAppendTime The log append time (if used) of the message set, otherwise Message.NoTimestamp
 * @param sourceCodec The source codec used in the message set (send by the producer)
 * @param targetCodec The target codec of the message set(after applying the broker compression configuration if any)
 * @param shallowCount The number of shallow messages
 * @param validBytes The number of valid bytes
 * @param offsetsMonotonic Are the offsets in this message set monotonically increasing
 */
case class LogAppendInfo(var firstOffset: Long,
                         var lastOffset: Long,
                         var maxTimestamp: Long,
                         var offsetOfMaxTimestamp: Long,
                         var logAppendTime: Long,
                         sourceCodec: CompressionCodec,
                         targetCodec: CompressionCodec,
                         shallowCount: Int,
                         validBytes: Int,
                         offsetsMonotonic: Boolean)

/**
 * A class used to hold useful metadata about a completed transaction. This is used to build
 * the transaction index after appending to the log.
 *
 * @param producerId The ID of the producer
 * @param firstOffset The first offset (inclusive) of the transaction
 * @param lastOffset The last offset (inclusive) of the transaction. This is always the offset of the
 *                   COMMIT/ABORT control record which indicates the transaction's completion.
 * @param isAborted Whether or not the transaction was aborted
 */
case class CompletedTxn(producerId: Long, firstOffset: Long, lastOffset: Long, isAborted: Boolean) {
  override def toString: String = {
    "CompletedTxn(" +
      s"producerId=$producerId, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset, " +
      s"isAborted=$isAborted)"
  }
}

/**
 * An append-only log for storing messages.
 *
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 *
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 *
 * @param dir The directory in which log segments are created.
 * @param config The log configuration settings
 * @param logStartOffset The earliest offset allowed to be exposed to kafka client.
 *                       The logStartOffset can be updated by :
 *                       - user's DeleteRecordsRequest
 *                       - broker's log retention
 *                       - broker's log truncation
 *                       The logStartOffset is used to decide the following:
 *                       - Log deletion. LogSegment whose nextOffset <= log's logStartOffset can be deleted.
 *                         It may trigger log rolling if the active segment is deleted.
 *                       - Earliest offset of the log in response to ListOffsetRequest. To avoid OffsetOutOfRange exception after user seeks to earliest offset,
 *                         we make sure that logStartOffset <= log's highWatermark
 *                       Other activities such as log cleaning are not affected by logStartOffset.
 * @param recoveryPoint The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
 * @param scheduler The thread pool scheduler used for background actions
 * @param brokerTopicStats Container for Broker Topic Yammer Metrics
 * @param time The time instance used for checking the clock
 * @param maxProducerIdExpirationMs The maximum amount of time to wait before a producer id is considered expired
 * @param producerIdExpirationCheckIntervalMs How often to check for producer ids which need to be expired
 */
@threadsafe
//管理LogSegment
class Log(@volatile var dir: File,//Log对应的磁盘目录
          @volatile var config: LogConfig,//log相关配置
          @volatile var logStartOffset: Long,
          @volatile var recoveryPoint: Long,//指定恢复操作的起始offset
          scheduler: Scheduler,//执行flush方法的
          brokerTopicStats: BrokerTopicStats,
          time: Time,
          val maxProducerIdExpirationMs: Int,
          val producerIdExpirationCheckIntervalMs: Int,
          val topicPartition: TopicPartition,
          val producerStateManager: ProducerStateManager) extends Logging with KafkaMetricsGroup {

  import kafka.log.Log._

  /* A lock that guards all modifications to the log */
  private val lock = new Object

  /* last time it was flushed */
  private val lastflushedTime = new AtomicLong(time.milliseconds)

  def initFileSize() : Int = {
    if (config.preallocate)
      config.segmentSize
    else
      0
  }
  //用于产生分配给消息得offset 同时也是当前副本的LEO
  @volatile private var nextOffsetMetadata: LogOffsetMetadata = _

  /* The earliest offset which is part of an incomplete transaction. This is used to compute the
   * last stable offset (LSO) in ReplicaManager. Note that it is possible that the "true" first unstable offset
   * gets removed from the log (through record or segment deletion). In this case, the first unstable offset
   * will point to the log start offset, which may actually be either part of a completed transaction or not
   * part of a transaction at all. However, since we only use the LSO for the purpose of restricting the
   * read_committed consumer to fetching decided data (i.e. committed, aborted, or non-transactional), this
   * temporary abuse seems justifiable and saves us from scanning the log after deletion to find the first offsets
   * of each ongoing transaction in order to compute a new first unstable offset. It is possible, however,
   * that this could result in disagreement between replicas depending on when they began replicating the log.
   * In the worst case, the LSO could be seen by a consumer to go backwards. 
   */
  @volatile var firstUnstableOffset: Option[LogOffsetMetadata] = None

  /* the actual segments of the log */
  //使用跳表对LogSegment进行管理
  //offset作为key LogSegment作为value
  //写入消息顺序写入，只有最后一个才能进行写操作，其他只读
  private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]

  val leaderEpochCache: LeaderEpochCache = initializeLeaderEpochCache()

  locally {
    val startMs = time.milliseconds

    loadSegments()

    /* Calculate the offset of the next message */
    nextOffsetMetadata = new LogOffsetMetadata(activeSegment.nextOffset, activeSegment.baseOffset, activeSegment.size)

    leaderEpochCache.clearAndFlushLatest(nextOffsetMetadata.messageOffset)

    logStartOffset = math.max(logStartOffset, segments.firstEntry().getValue.baseOffset)

    // The earliest leader epoch may not be flushed during a hard failure. Recover it here.
    leaderEpochCache.clearAndFlushEarliest(logStartOffset)

    loadProducerState(logEndOffset, reloadFromCleanShutdown = hasCleanShutdownFile)

    info("Completed load of log %s with %d log segments, log start offset %d and log end offset %d in %d ms"
      .format(name, segments.size(), logStartOffset, logEndOffset, time.milliseconds - startMs))
  }

  private val tags = Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString)
  // 在log类初始化，执行下面的代码创建Gauge对象
  newGauge("NumLogSegments",
    new Gauge[Int] {
      def value = numberOfSegments
    },
    tags)

  newGauge("LogStartOffset",
    new Gauge[Long] {
      def value = logStartOffset
    },
    tags)

  newGauge("LogEndOffset",
    new Gauge[Long] {
      def value = logEndOffset
    },
    tags)

  newGauge("Size",
    new Gauge[Long] {
      def value = size
    },
    tags)

  scheduler.schedule(name = "PeriodicProducerExpirationCheck", fun = () => {
    lock synchronized {
      producerStateManager.removeExpiredProducers(time.milliseconds)
    }
  }, period = producerIdExpirationCheckIntervalMs, delay = producerIdExpirationCheckIntervalMs, unit = TimeUnit.MILLISECONDS)

  /** The name of this log */
  def name  = dir.getName()

  private def initializeLeaderEpochCache(): LeaderEpochCache = {
    // create the log directory if it doesn't exist
    Files.createDirectories(dir.toPath)
    new LeaderEpochFileCache(topicPartition, () => logEndOffsetMetadata,
      new LeaderEpochCheckpointFile(LeaderEpochFile.newFile(dir)))
  }

  private def removeTempFilesAndCollectSwapFiles(): Set[File] = {
    var swapFiles = Set[File]()

    for (file <- dir.listFiles if file.isFile) {
      if(!file.canRead)
        throw new IOException("Could not read file " + file)
      val filename = file.getName
      //删除 .delete 和 .cleaned后缀文件
      if(filename.endsWith(DeletedFileSuffix) || filename.endsWith(CleanedFileSuffix)) {
        // if the file ends in .deleted or .cleaned, delete it
        Files.deleteIfExists(file.toPath)
      } else if(filename.endsWith(SwapFileSuffix)) {//以 .swap结尾
        // we crashed in the middle of a swap operation, to recover:
        // if a log, delete the .index file, complete the swap operation later
        // if an index just delete it, it will be rebuilt
        val baseFile = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
        //如果是索引文件直接删除
        if (isIndexFile(baseFile)) {
          Files.deleteIfExists(file.toPath)
        } else if (isLogFile(baseFile)) {
          //如果是日志文件则恢复
          // delete the index files
          val offset = offsetFromFilename(baseFile.getName)
          Files.deleteIfExists(Log.offsetIndexFile(dir, offset).toPath)
          Files.deleteIfExists(Log.timeIndexFile(dir, offset).toPath)
          Files.deleteIfExists(Log.transactionIndexFile(dir, offset).toPath)
          //添加到swapFiles中 待后面恢复
          swapFiles += file
        }
      }
    }
    swapFiles
  }

  private def loadSegmentFiles(): Unit = {
    //遍历所有文件
    for (file <- dir.listFiles.sortBy(_.getName) if file.isFile) {
      val filename = file.getName
      //处理索引文件
      if (isIndexFile(file)) {
        // if it is an index file, make sure it has a corresponding .log file
        val offset = offsetFromFilename(filename)
        val logFile = Log.logFile(dir, offset)
        //不存在日志文件则删除
        if (!logFile.exists) {
          warn("Found an orphaned index file, %s, with no corresponding log file.".format(file.getAbsolutePath))
          Files.deleteIfExists(file.toPath)
        }
      } else if (isLogFile(file)) {//处理日志文件
        // if it's a log file, load the corresponding log segment
        val startOffset = offsetFromFilename(filename)
        val indexFile = Log.offsetIndexFile(dir, startOffset)
        val timeIndexFile = Log.timeIndexFile(dir, startOffset)
        val txnIndexFile = Log.transactionIndexFile(dir, startOffset)

        val indexFileExists = indexFile.exists()
        val timeIndexFileExists = timeIndexFile.exists()
        val segment = new LogSegment(dir = dir,
          startOffset = startOffset,
          indexIntervalBytes = config.indexInterval,
          maxIndexSize = config.maxIndexSize,
          rollJitterMs = config.randomSegmentJitter,
          time = time,
          fileAlreadyExists = true)
        //索引存在
        if (indexFileExists) {
          try {
            //检查索引文件的完整性
            segment.index.sanityCheck()
            // Resize the time index file to 0 if it is newly created.
            if (!timeIndexFileExists)
              segment.timeIndex.resize(0)
            segment.timeIndex.sanityCheck()
            segment.txnIndex.sanityCheck()
          } catch {
            case e: java.lang.IllegalArgumentException =>
              warn(s"Found a corrupted index file due to ${e.getMessage}}. deleting ${timeIndexFile.getAbsolutePath}, " +
                s"${indexFile.getAbsolutePath}, and ${txnIndexFile.getAbsolutePath} and rebuilding index...")
              Files.deleteIfExists(timeIndexFile.toPath)
              Files.delete(indexFile.toPath)
              segment.txnIndex.delete()
              recoverSegment(segment)
          }
        } else {
          error("Could not find offset index file corresponding to log file %s, rebuilding index...".format(segment.log.file.getAbsolutePath))
          //如果没有对应的索引文件，则重新构建索引文件
          recoverSegment(segment)
        }
        //将segment放入跳表中保存
        segments.put(startOffset, segment)
      }
    }
  }

  private def recoverSegment(segment: LogSegment, leaderEpochCache: Option[LeaderEpochCache] = None): Int = lock synchronized {
    val stateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)
    stateManager.truncateAndReload(logStartOffset, segment.baseOffset, time.milliseconds)
    logSegments(stateManager.mapEndOffset, segment.baseOffset).foreach { segment =>
      val startOffset = math.max(segment.baseOffset, stateManager.mapEndOffset)
      val fetchDataInfo = segment.read(startOffset, None, Int.MaxValue)
      if (fetchDataInfo != null)
        loadProducersFromLog(stateManager, fetchDataInfo.records)
    }
    stateManager.updateMapEndOffset(segment.baseOffset)
    val bytesTruncated = segment.recover(stateManager, leaderEpochCache)

    // once we have recovered the segment's data, take a snapshot to ensure that we won't
    // need to reload the same segment again while recovering another segment.
    stateManager.takeSnapshot()
    bytesTruncated
  }

  private def completeSwapOperations(swapFiles: Set[File]): Unit = {
    for (swapFile <- swapFiles) {
      val logFile = new File(CoreUtils.replaceSuffix(swapFile.getPath, SwapFileSuffix, ""))
      val filename = logFile.getName
      //根据日志名称得到baseOffset
      val startOffset = offsetFromFilename(filename)
      val indexFile = new File(CoreUtils.replaceSuffix(logFile.getPath, LogFileSuffix, IndexFileSuffix) + SwapFileSuffix)
      val index =  new OffsetIndex(indexFile, baseOffset = startOffset, maxIndexSize = config.maxIndexSize)
      val timeIndexFile = new File(CoreUtils.replaceSuffix(logFile.getPath, LogFileSuffix, TimeIndexFileSuffix) + SwapFileSuffix)
      val timeIndex = new TimeIndex(timeIndexFile, baseOffset = startOffset, maxIndexSize = config.maxIndexSize)
      val txnIndexFile = new File(CoreUtils.replaceSuffix(logFile.getPath, LogFileSuffix, TxnIndexFileSuffix) + SwapFileSuffix)
      val txnIndex = new TransactionIndex(startOffset, txnIndexFile)
      //创建LogSegment
      val swapSegment = new LogSegment(FileRecords.open(swapFile),
        index = index,
        timeIndex = timeIndex,
        txnIndex = txnIndex,
        baseOffset = startOffset,
        indexIntervalBytes = config.indexInterval,
        rollJitterMs = config.randomSegmentJitter,
        time = time)
      info("Found log file %s from interrupted swap operation, repairing.".format(swapFile.getPath))
      //重建索引文件
      recoverSegment(swapSegment)
      //查找swapSement对应的日志压缩钱的LogSegment集合
      val oldSegments = logSegments(swapSegment.baseOffset, swapSegment.nextOffset())
      //将swapSegment对象加入到segments跳表中管理
      //将oldSegments中的集合从segment中删掉，并删除对应的文件和索引文件
      //将文件的 .swap后缀名去掉
      replaceSegments(swapSegment, oldSegments.toSeq, isRecoveredSwapFile = true)
    }
  }

  /* Load the log segments from the log files on disk */
  private def loadSegments() {

    //处理 .delete .clean .swap文件
    val swapFiles = removeTempFilesAndCollectSwapFiles()

    //加载全部的日志文件和索引文件
    //如果索引文件没有配对的日志文件，则删除索引文件
    //如果日志文件中没有对应的索引文件，则重建索引
    loadSegmentFiles()

    // Finally, complete any interrupted swap operations. To be crash-safe,
    // log files that are replaced by the swap segment should be renamed to .deleted
    // before the swap file is restored as the new segment file.
    //处理swapFiles
    completeSwapOperations(swapFiles)

    //对于空log 创建activeSegment
    if(logSegments.isEmpty) {
      // no existing segments, create a new mutable segment beginning at offset 0
      segments.put(0L, new LogSegment(dir = dir,
                                     startOffset = 0,
                                     indexIntervalBytes = config.indexInterval,
                                     maxIndexSize = config.maxIndexSize,
                                     rollJitterMs = config.randomSegmentJitter,
                                     time = time,
                                     fileAlreadyExists = false,
                                     initFileSize = this.initFileSize(),
                                     preallocate = config.preallocate))
      //对于非空的Log则进行恢复操作
    } else if (!dir.getAbsolutePath.endsWith(Log.DeleteDirSuffix)) {
      recoverLog()
      // reset the index size of the currently active log segment to allow more entries
      activeSegment.index.resize(config.maxIndexSize)
      activeSegment.timeIndex.resize(config.maxIndexSize)
    }
  }

  private def updateLogEndOffset(messageOffset: Long) {
    nextOffsetMetadata = new LogOffsetMetadata(messageOffset, activeSegment.baseOffset, activeSegment.size)
  }

  //负责处理Broker非正常关闭导致的消息异常，将recoveryPoint-activeSegment中所有消息进行验证，将失败消息截断
  private def recoverLog() {
    // if we have the clean shutdown marker, skip recovery
    //正常关闭，不需要
    if(hasCleanShutdownFile) {
      this.recoveryPoint = activeSegment.nextOffset()
      return
    }

    // okay we need to actually recovery this log
    //获得全部未刷新的Logsegment即 revocery之后的所有的
    val unflushed = logSegments(this.recoveryPoint, Long.MaxValue).iterator
    while(unflushed.hasNext) {
      val segment = unflushed.next
      info("Recovering unflushed segment %d in log %s.".format(segment.baseOffset, name))
      val truncatedBytes =
        try {
          //重建索引并验证日志文件，验证失败的部分则截掉
          recoverSegment(segment, Some(leaderEpochCache))
        } catch {
          case _: InvalidOffsetException =>
            val startOffset = segment.baseOffset
            warn("Found invalid offset during recovery for log " + dir.getName +". Deleting the corrupt segment and " +
                 "creating an empty one with starting offset " + startOffset)
            segment.truncateTo(startOffset)
        }
      //如果有验证失败的消息，将剩余的LogSegemen删除
      if(truncatedBytes > 0) {
        // we had an invalid message, delete all remaining log
        warn("Corruption found in segment %d of log %s, truncating to offset %d.".format(segment.baseOffset, name,
          segment.nextOffset()))
        unflushed.foreach(deleteSegment)
      }
    }
  }

  private def loadProducerState(lastOffset: Long, reloadFromCleanShutdown: Boolean): Unit = lock synchronized {
    val messageFormatVersion = config.messageFormatVersion.messageFormatVersion
    info(s"Loading producer state from offset $lastOffset for partition $topicPartition with message " +
      s"format version $messageFormatVersion")

    // We want to avoid unnecessary scanning of the log to build the producer state when the broker is being
    // upgraded. The basic idea is to use the absence of producer snapshot files to detect the upgrade case,
    // but we have to be careful not to assume too much in the presence of broker failures. The two most common
    // upgrade cases in which we expect to find no snapshots are the following:
    //
    // 1. The broker has been upgraded, but the topic is still on the old message format.
    // 2. The broker has been upgraded, the topic is on the new message format, and we had a clean shutdown.
    //
    // If we hit either of these cases, we skip producer state loading and write a new snapshot at the log end
    // offset (see below). The next time the log is reloaded, we will load producer state using this snapshot
    // (or later snapshots). Otherwise, if there is no snapshot file, then we have to rebuild producer state
    // from the first segment.

    if (producerStateManager.latestSnapshotOffset.isEmpty && (messageFormatVersion < RecordBatch.MAGIC_VALUE_V2 || reloadFromCleanShutdown)) {
      // To avoid an expensive scan through all of the segments, we take empty snapshots from the start of the
      // last two segments and the last offset. This should avoid the full scan in the case that the log needs
      // truncation.
      val nextLatestSegmentBaseOffset = Option(segments.lowerEntry(activeSegment.baseOffset)).map(_.getValue.baseOffset)
      val offsetsToSnapshot = Seq(nextLatestSegmentBaseOffset, Some(activeSegment.baseOffset), Some(lastOffset))
      offsetsToSnapshot.flatten.foreach { offset =>
        producerStateManager.updateMapEndOffset(offset)
        producerStateManager.takeSnapshot()
      }
    } else {
      val isEmptyBeforeTruncation = producerStateManager.isEmpty && producerStateManager.mapEndOffset >= lastOffset
      producerStateManager.truncateAndReload(logStartOffset, lastOffset, time.milliseconds())

      // Only do the potentially expensive reloading if the last snapshot offset is lower than the log end
      // offset (which would be the case on first startup) and there were active producers prior to truncation
      // (which could be the case if truncating after initial loading). If there weren't, then truncating
      // shouldn't change that fact (although it could cause a producerId to expire earlier than expected),
      // and we can skip the loading. This is an optimization for users which are not yet using
      // idempotent/transactional features yet.
      if (lastOffset > producerStateManager.mapEndOffset && !isEmptyBeforeTruncation) {
        logSegments(producerStateManager.mapEndOffset, lastOffset).foreach { segment =>
          val startOffset = Utils.max(segment.baseOffset, producerStateManager.mapEndOffset, logStartOffset)
          producerStateManager.updateMapEndOffset(startOffset)
          producerStateManager.takeSnapshot()

          val fetchDataInfo = segment.read(startOffset, Some(lastOffset), Int.MaxValue)
          if (fetchDataInfo != null)
            loadProducersFromLog(producerStateManager, fetchDataInfo.records)
        }
      }

      producerStateManager.updateMapEndOffset(lastOffset)
      updateFirstUnstableOffset()
    }
  }

  private def loadProducersFromLog(producerStateManager: ProducerStateManager, records: Records): Unit = {
    val loadedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    records.batches.asScala.foreach { batch =>
      if (batch.hasProducerId) {
        val maybeCompletedTxn = updateProducers(batch, loadedProducers, loadingFromLog = true)
        maybeCompletedTxn.foreach(completedTxns += _)
      }
    }
    loadedProducers.values.foreach(producerStateManager.update)
    completedTxns.foreach(producerStateManager.completeTxn)
  }

  private[log] def activeProducers: Map[Long, ProducerIdEntry] = lock synchronized {
    producerStateManager.activeProducers
  }

  /**
   * Check if we have the "clean shutdown" file
   */
  private def hasCleanShutdownFile = new File(dir.getParentFile, CleanShutdownFile).exists()

  /**
   * The number of segments in the log.
   * Take care! this is an O(n) operation.
   */
  def numberOfSegments: Int = segments.size

  /**
   * Close this log
   */
  def close() {
    debug(s"Closing log $name")
    lock synchronized {
      // We take a snapshot at the last written offset to hopefully avoid the need to scan the log
      // after restarting and to ensure that we cannot inadvertently hit the upgrade optimization
      // (the clean shutdown file is written after the logs are all closed).
      producerStateManager.takeSnapshot()
      logSegments.foreach(_.close())
    }
  }

  /**
   * Append this message set to the active segment of the log, assigning offsets and Partition Leader Epochs
   * @param records The records to append
   * @param isFromClient Whether or not this append is from a producer
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  def appendAsLeader(records: MemoryRecords, leaderEpoch: Int, isFromClient: Boolean = true): LogAppendInfo = {
    append(records, isFromClient, assignOffsets = true, leaderEpoch)
  }

  /**
   * Append this message set to the active segment of the log without assigning offsets or Partition Leader Epochs
   * @param records The records to append
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  def appendAsFollower(records: MemoryRecords): LogAppendInfo = {
    append(records, isFromClient = false, assignOffsets = false, leaderEpoch = -1)
  }

  /**
   * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
   *
   * This method will generally be responsible for assigning offsets to the messages,
   * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
   *
   * @param records The log records to append
   * @param isFromClient Whether or not this append is from a producer
   * @param assignOffsets Should the log assign offsets to this message set or blindly apply what it is given
   * @param leaderEpoch The partition's leader epoch which will be applied to messages when offsets are assigned on the leader
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  //向Log中追加消息
  private def append(records: MemoryRecords, isFromClient: Boolean, assignOffsets: Boolean, leaderEpoch: Int): LogAppendInfo = {
    //检查消息长度和crc32校验码
    val appendInfo = analyzeAndValidateRecords(records, isFromClient = isFromClient)

    // return if we have no valid messages or if this is a duplicate of the last appended entry
    if (appendInfo.shallowCount == 0)
      return appendInfo

    // trim any invalid bytes or partial messages before appending it to the on-disk log
    //外层消息检查，根据appendInfo对未通过消息验证的消息截断
    var validRecords = trimInvalidBytes(records, appendInfo)

    try {
      // they are valid, insert them in the log
      lock synchronized {
        //判断是否需要分配offset
        if (assignOffsets) {
          // assign offsets to the message set
          //获得起始offset
          val offset = new LongRef(nextOffsetMetadata.messageOffset)
          appendInfo.firstOffset = offset.value
          val now = time.milliseconds
          //进行进一步的校验
          val validateAndOffsetAssignResult = try {
            LogValidator.validateMessagesAndAssignOffsets(validRecords,
              offset,
              now,
              appendInfo.sourceCodec,
              appendInfo.targetCodec,
              config.compact,
              config.messageFormatVersion.messageFormatVersion,
              config.messageTimestampType,
              config.messageTimestampDifferenceMaxMs,
              leaderEpoch,
              isFromClient)
          } catch {
            case e: IOException => throw new KafkaException("Error in validating messages while appending to log '%s'".format(name), e)
          }
          validRecords = validateAndOffsetAssignResult.validatedRecords
          appendInfo.maxTimestamp = validateAndOffsetAssignResult.maxTimestamp
          appendInfo.offsetOfMaxTimestamp = validateAndOffsetAssignResult.shallowOffsetOfMaxTimestamp
          appendInfo.lastOffset = offset.value - 1
          //修改时间戳
          if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME)
            appendInfo.logAppendTime = now

          // re-validate message sizes if there's a possibility that they have changed (due to re-compression or message
          // format conversion)
          if (validateAndOffsetAssignResult.messageSizeMaybeChanged) {
            for (batch <- validRecords.batches.asScala) {
              if (batch.sizeInBytes > config.maxMessageSize) {
                // we record the original message set size instead of the trimmed size
                // to be consistent with pre-compression bytesRejectedRate recording
                brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
                brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
                throw new RecordTooLargeException("Message batch size is %d bytes which exceeds the maximum configured size of %d."
                  .format(batch.sizeInBytes, config.maxMessageSize))
              }
            }
          }
        } else {
          // we are taking the offsets we are given
          if (!appendInfo.offsetsMonotonic || appendInfo.firstOffset < nextOffsetMetadata.messageOffset)
            throw new IllegalArgumentException("Out of order offsets found in " + records.records.asScala.map(_.offset))
        }

        // update the epoch cache with the epoch stamped onto the message by the leader
        validRecords.batches.asScala.foreach { batch =>
          if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
            leaderEpochCache.assign(batch.partitionLeaderEpoch, batch.baseOffset)
        }

        // check messages set size may be exceed config.segmentSize
        if (validRecords.sizeInBytes > config.segmentSize) {
          throw new RecordBatchTooLargeException("Message batch size is %d bytes which exceeds the maximum configured segment size of %d."
            .format(validRecords.sizeInBytes, config.segmentSize))
        }

        // now that we have valid records, offsets assigned, and timestamps updated, we need to
        // validate the idempotent/transactional state of the producers and collect some metadata
        val (updatedProducers, completedTxns, maybeDuplicate) = analyzeAndValidateProducerState(validRecords, isFromClient)
        maybeDuplicate.foreach { duplicate =>
          appendInfo.firstOffset = duplicate.firstOffset
          appendInfo.lastOffset = duplicate.lastOffset
          appendInfo.logAppendTime = duplicate.timestamp
          return appendInfo
        }

        // maybe roll the log if this segment is full
        //获得activeSegment
        //当前activeSegment的日志大小加上上次待追加的消息大小超过Logsegment的最大长度
        //当前activeSegment的寿命超过了LogSegment配置的最长存活时间
        //索引文件满了
        //以上会创建新的segment
        val segment = maybeRoll(messagesSize = validRecords.sizeInBytes,
          maxTimestampInMessages = appendInfo.maxTimestamp,
          maxOffsetInMessages = appendInfo.lastOffset)

        val logOffsetMetadata = LogOffsetMetadata(
          messageOffset = appendInfo.firstOffset,
          segmentBaseOffset = segment.baseOffset,
          relativePositionInSegment = segment.size)
        //追加消息
        segment.append(firstOffset = appendInfo.firstOffset,
          largestOffset = appendInfo.lastOffset,
          largestTimestamp = appendInfo.maxTimestamp,
          shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,
          records = validRecords)

        // update the producer state
        for ((producerId, producerAppendInfo) <- updatedProducers) {
          producerAppendInfo.maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata)
          producerStateManager.update(producerAppendInfo)
        }

        // update the transaction index with the true last stable offset. The last offset visible
        // to consumers using READ_COMMITTED will be limited by this value and the high watermark.
        for (completedTxn <- completedTxns) {
          val lastStableOffset = producerStateManager.completeTxn(completedTxn)
          segment.updateTxnIndex(completedTxn, lastStableOffset)
        }

        // always update the last producer id map offset so that the snapshot reflects the current offset
        // even if there isn't any idempotent data being written
        producerStateManager.updateMapEndOffset(appendInfo.lastOffset + 1)

        // increment the log end offset
        //更新LEO
        updateLogEndOffset(appendInfo.lastOffset + 1)

        // update the first unstable offset (which is used to compute LSO)
        updateFirstUnstableOffset()

        trace("Appended message set to log %s with first offset: %d, next offset: %d, and messages: %s"
          .format(this.name, appendInfo.firstOffset, nextOffsetMetadata.messageOffset, validRecords))
        //未刷到磁盘的数据达到阈值时候，调用flush方法刷新
        if (unflushedMessages >= config.flushInterval)
          //执行flush方法
          flush()

        appendInfo
      }
    } catch {
      case e: IOException => throw new KafkaStorageException("I/O exception in append to log '%s'".format(name), e)
    }
  }

  def onHighWatermarkIncremented(highWatermark: Long): Unit = {
    lock synchronized {
      producerStateManager.onHighWatermarkUpdated(highWatermark)
      updateFirstUnstableOffset()
    }
  }

  private def updateFirstUnstableOffset(): Unit = lock synchronized {
    val updatedFirstStableOffset = producerStateManager.firstUnstableOffset match {
      case Some(logOffsetMetadata) if logOffsetMetadata.messageOffsetOnly || logOffsetMetadata.messageOffset < logStartOffset =>
        val offset = math.max(logOffsetMetadata.messageOffset, logStartOffset)
        val segment = segments.floorEntry(offset).getValue
        val position  = segment.translateOffset(offset)
        Some(LogOffsetMetadata(offset, segment.baseOffset, position.position))
      case other => other
    }

    if (updatedFirstStableOffset != this.firstUnstableOffset) {
      debug(s"First unstable offset for ${this.name} updated to $updatedFirstStableOffset")
      this.firstUnstableOffset = updatedFirstStableOffset
    }
  }

  /**
   * Increment the log start offset if the provided offset is larger.
   */
  def maybeIncrementLogStartOffset(offset: Long) {
    // We don't have to write the log start offset to log-start-offset-checkpoint immediately.
    // The deleteRecordsOffset may be lost only if all in-sync replicas of this broker are shutdown
    // in an unclean manner within log.flush.start.offset.checkpoint.interval.ms. The chance of this happening is low.
    lock synchronized {
      if (offset > logStartOffset) {
        logStartOffset = offset
        leaderEpochCache.clearAndFlushEarliest(logStartOffset)
        producerStateManager.truncateHead(logStartOffset)
        updateFirstUnstableOffset()
      }
    }
  }

  private def analyzeAndValidateProducerState(records: MemoryRecords, isFromClient: Boolean):
  (mutable.Map[Long, ProducerAppendInfo], List[CompletedTxn], Option[ProducerIdEntry]) = {
    val updatedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    for (batch <- records.batches.asScala if batch.hasProducerId) {
      val maybeLastEntry = producerStateManager.lastEntry(batch.producerId)

      // if this is a client produce request, there will be only one batch. If that batch matches
      // the last appended entry for that producer, then this request is a duplicate and we return
      // the last appended entry to the client.
      if (isFromClient && maybeLastEntry.exists(_.isDuplicate(batch)))
        return (updatedProducers, completedTxns.toList, maybeLastEntry)

      val maybeCompletedTxn = updateProducers(batch, updatedProducers, loadingFromLog = false)
      maybeCompletedTxn.foreach(completedTxns += _)
    }
    (updatedProducers, completedTxns.toList, None)
  }

  /**
   * Validate the following:
   * <ol>
   * <li> each message matches its CRC
   * <li> each message size is valid
   * <li> that the sequence numbers of the incoming record batches are consistent with the existing state and with each other.
   * </ol>
   *
   * Also compute the following quantities:
   * <ol>
   * <li> First offset in the message set
   * <li> Last offset in the message set
   * <li> Number of messages
   * <li> Number of valid bytes
   * <li> Whether the offsets are monotonically increasing
   * <li> Whether any compression codec is used (if many are used, then the last one is given)
   * </ol>
   */
  private def analyzeAndValidateRecords(records: MemoryRecords, isFromClient: Boolean): LogAppendInfo = {
    var shallowMessageCount = 0//记录外层消息数
    var validBytesCount = 0//记录验证的records的字节数之和
    var firstOffset = -1L//第一条记录offset
    var lastOffset = -1L//最后一条offset
    var sourceCodec: CompressionCodec = NoCompressionCodec
    var monotonic = true//标示生产者为消息分配的内部offset是否单调递增
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    //浅层遍历，不会解压缩
    for (batch <- records.batches.asScala) {
      // we only validate V2 and higher to avoid potential compatibility issues with older clients
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2 && isFromClient && batch.baseOffset != 0)
        throw new InvalidRecordException(s"The baseOffset of the record batch should be 0, but it is ${batch.baseOffset}")

      // update the first offset if on the first message. For magic versions older than 2, we use the last offset
      // to avoid the need to decompress the data (the last offset can be obtained directly from the wrapper message).
      // For magic version 2, we can get the first offset directly from the batch header.
      // When appending to the leader, we will update LogAppendInfo.baseOffset with the correct value. In the follower
      // case, validation will be more lenient.
      if (firstOffset < 0)
        firstOffset = if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) batch.baseOffset else batch.lastOffset

      // check that offsets are monotonically increasing
      if (lastOffset >= batch.lastOffset)
        monotonic = false

      // update the last offset seen
      lastOffset = batch.lastOffset

      // Check if the message sizes are valid.
      val batchSize = batch.sizeInBytes
      if (batchSize > config.maxMessageSize) {
        brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
        brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
        throw new RecordTooLargeException(s"The record batch size is $batchSize bytes which exceeds the maximum configured " +
          s"value of ${config.maxMessageSize}.")
      }

      // check the validity of the message by checking CRC
      batch.ensureValid()

      if (batch.maxTimestamp > maxTimestamp) {
        maxTimestamp = batch.maxTimestamp
        offsetOfMaxTimestamp = lastOffset
      }

      shallowMessageCount += 1
      validBytesCount += batchSize

      val messageCodec = CompressionCodec.getCompressionCodec(batch.compressionType.id)
      //记录生产真采用的压缩方式
      if (messageCodec != NoCompressionCodec)
        sourceCodec = messageCodec
    }

    // Apply broker-side compression if any
    val targetCodec = BrokerCompressionCodec.getTargetCompressionCodec(config.compressionType, sourceCodec)
    LogAppendInfo(firstOffset, lastOffset, maxTimestamp, offsetOfMaxTimestamp, RecordBatch.NO_TIMESTAMP, sourceCodec,
      targetCodec, shallowMessageCount, validBytesCount, monotonic)
  }

  private def updateProducers(batch: RecordBatch,
                              producers: mutable.Map[Long, ProducerAppendInfo],
                              loadingFromLog: Boolean): Option[CompletedTxn] = {
    val producerId = batch.producerId
    val appendInfo = producers.getOrElseUpdate(producerId, producerStateManager.prepareUpdate(producerId, loadingFromLog))
    appendInfo.append(batch)
  }

  /**
   * Trim any invalid bytes from the end of this message set (if there are any)
   *
   * @param records The records to trim
   * @param info The general information of the message set
   * @return A trimmed message set. This may be the same as what was passed in or it may not.
   */
  private def trimInvalidBytes(records: MemoryRecords, info: LogAppendInfo): MemoryRecords = {
    val validBytes = info.validBytes
    if (validBytes < 0)
      throw new CorruptRecordException("Illegal length of message set " + validBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests")
    if (validBytes == records.sizeInBytes) {
      records
    } else {
      // trim invalid bytes
      val validByteBuffer = records.buffer.duplicate()
      validByteBuffer.limit(validBytes)
      MemoryRecords.readableRecords(validByteBuffer)
    }
  }

  private[log] def readUncommitted(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None,
                                   minOneMessage: Boolean = false): FetchDataInfo = {
    read(startOffset, maxLength, maxOffset, minOneMessage, isolationLevel = IsolationLevel.READ_UNCOMMITTED)
  }

  /**
   * Read messages from the log.
   *
   * @param startOffset The offset to begin reading at
   * @param maxLength The maximum number of bytes to read
   * @param maxOffset The offset to read up to, exclusive. (i.e. this offset NOT included in the resulting message set)
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
   * @param isolationLevel The isolation level of the fetcher. The READ_UNCOMMITTED isolation level has the traditional
   *                       read semantics (e.g. consumers are limited to fetching up to the high watermark). In
   *                       READ_COMMITTED, consumers are limited to fetching up to the last stable offset. Additionally,
   *                       in READ_COMMITTED, the transaction index is consulted after fetching to collect the list
   *                       of aborted transactions in the fetch range which the consumer uses to filter the fetched
   *                       records before they are returned to the user. Note that fetches from followers always use
   *                       READ_UNCOMMITTED.
   *
   * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the log start offset
   * @return The fetch data information including fetch starting offset metadata and messages read.
   */
  //用来读取消息
  def read(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None, minOneMessage: Boolean = false,
           isolationLevel: IsolationLevel): FetchDataInfo = {
    trace("Reading %d bytes from offset %d in log %s of length %d bytes".format(maxLength, startOffset, name, size))

    // Because we don't use lock for reading, the synchronization is a little bit tricky.
    // We create the local variables to avoid race conditions with updates to the log.
    val currentNextOffsetMetadata = nextOffsetMetadata
    val next = currentNextOffsetMetadata.messageOffset
    if (startOffset == next) {//边界检查
      val abortedTransactions =
        if (isolationLevel == IsolationLevel.READ_COMMITTED) Some(List.empty[AbortedTransaction])
        else None
      return FetchDataInfo(currentNextOffsetMetadata, MemoryRecords.EMPTY, firstEntryIncomplete = false,
        abortedTransactions = abortedTransactions)
    }
    //查找baseoffset < startOffset且baseOffset最大的LogSegment
    var segmentEntry = segments.floorEntry(startOffset)

    // return error on attempt to read beyond the log end offset or read below log start offset
    if (startOffset > next || segmentEntry == null || startOffset < logStartOffset)
      throw new OffsetOutOfRangeException("Request for offset %d but we only have log segments in the range %d to %d.".format(startOffset, logStartOffset, next))

    // Do the read on the segment with a base offset less than the target offset
    // but if that segment doesn't contain any messages with an offset greater than that
    // continue to read from successive segments until we get some messages or we reach the end of the log
    while(segmentEntry != null) {
      val segment = segmentEntry.getValue

      // If the fetch occurs on the active segment, there might be a race condition where two fetch requests occur after
      // the message is appended but before the nextOffsetMetadata is updated. In that case the second fetch may
      // cause OffsetOutOfRangeException. To solve that, we cap the reading up to exposed position instead of the log
      // end of the active segment.
      val maxPosition = {
        //处理读取activeSegment的情况
        if (segmentEntry == segments.lastEntry) {
          val exposedPos = nextOffsetMetadata.relativePositionInSegment.toLong
          // Check the segment again in case a new segment has just rolled out.
          //写线程并发进行了roll操作，变成了读取非activeSegment的场景
          if (segmentEntry != segments.lastEntry)
            // New log segment has rolled out, we can read up to the file end.
            segment.size
          else
            exposedPos
        } else {//读到的是非activeSegment的情况，直接读取到LogSegment结尾
          segment.size
        }
      }
      val fetchInfo = segment.read(startOffset, maxOffset, maxLength, maxPosition, minOneMessage)
      //在此LogSegment中没有读取到数据 ，则继续读取下一个LogSegment
      if (fetchInfo == null) {
        segmentEntry = segments.higherEntry(segmentEntry.getKey)
      } else {
        return isolationLevel match {
          case IsolationLevel.READ_UNCOMMITTED => fetchInfo
          case IsolationLevel.READ_COMMITTED => addAbortedTransactions(startOffset, segmentEntry, fetchInfo)
        }
      }
    }

    // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
    // this can happen when all messages with offset larger than start offsets have been deleted.
    // In this case, we will return the empty set with log end offset metadata
    FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY)
  }

  private[log] def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long): List[AbortedTxn] = {
    val segmentEntry = segments.floorEntry(startOffset)
    val allAbortedTxns = ListBuffer.empty[AbortedTxn]
    def accumulator(abortedTxns: List[AbortedTxn]): Unit = allAbortedTxns ++= abortedTxns
    collectAbortedTransactions(logStartOffset, upperBoundOffset, segmentEntry, accumulator)
    allAbortedTxns.toList
  }

  private def addAbortedTransactions(startOffset: Long, segmentEntry: JEntry[JLong, LogSegment],
                                     fetchInfo: FetchDataInfo): FetchDataInfo = {
    val fetchSize = fetchInfo.records.sizeInBytes
    val startOffsetPosition = OffsetPosition(fetchInfo.fetchOffsetMetadata.messageOffset,
      fetchInfo.fetchOffsetMetadata.relativePositionInSegment)
    val upperBoundOffset = segmentEntry.getValue.fetchUpperBoundOffset(startOffsetPosition, fetchSize).getOrElse {
      val nextSegmentEntry = segments.higherEntry(segmentEntry.getKey)
      if (nextSegmentEntry != null)
        nextSegmentEntry.getValue.baseOffset
      else
        logEndOffset
    }

    val abortedTransactions = ListBuffer.empty[AbortedTransaction]
    def accumulator(abortedTxns: List[AbortedTxn]): Unit = abortedTransactions ++= abortedTxns.map(_.asAbortedTransaction)
    collectAbortedTransactions(startOffset, upperBoundOffset, segmentEntry, accumulator)

    FetchDataInfo(fetchOffsetMetadata = fetchInfo.fetchOffsetMetadata,
      records = fetchInfo.records,
      firstEntryIncomplete = fetchInfo.firstEntryIncomplete,
      abortedTransactions = Some(abortedTransactions.toList))
  }

  private def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long,
                                         startingSegmentEntry: JEntry[JLong, LogSegment],
                                         accumulator: List[AbortedTxn] => Unit): Unit = {
    var segmentEntry = startingSegmentEntry
    while (segmentEntry != null) {
      val searchResult = segmentEntry.getValue.collectAbortedTxns(startOffset, upperBoundOffset)
      accumulator(searchResult.abortedTransactions)
      if (searchResult.isComplete)
        return
      segmentEntry = segments.higherEntry(segmentEntry.getKey)
    }
  }

  /**
   * Get an offset based on the given timestamp
   * The offset returned is the offset of the first message whose timestamp is greater than or equals to the
   * given timestamp.
   *
   * If no such message is found, the log end offset is returned.
   *
   * `NOTE:` OffsetRequest V0 does not use this method, the behavior of OffsetRequest V0 remains the same as before
   * , i.e. it only gives back the timestamp based on the last modification time of the log segments.
   *
   * @param targetTimestamp The given timestamp for offset fetching.
   * @return The offset of the first message whose timestamp is greater than or equals to the given timestamp.
   *         None if no such message is found.
   */
  def fetchOffsetsByTimestamp(targetTimestamp: Long): Option[TimestampOffset] = {
    debug(s"Searching offset for timestamp $targetTimestamp")

    if (config.messageFormatVersion < KAFKA_0_10_0_IV0 &&
        targetTimestamp != ListOffsetRequest.EARLIEST_TIMESTAMP &&
        targetTimestamp != ListOffsetRequest.LATEST_TIMESTAMP)
      throw new UnsupportedForMessageFormatException(s"Cannot search offsets based on timestamp because message format version " +
          s"for partition $topicPartition is ${config.messageFormatVersion} which is earlier than the minimum " +
          s"required version $KAFKA_0_10_0_IV0")

    // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
    // constant time access while being safe to use with concurrent collections unlike `toArray`.
    val segmentsCopy = logSegments.toBuffer
    // For the earliest and latest, we do not need to return the timestamp.
    if (targetTimestamp == ListOffsetRequest.EARLIEST_TIMESTAMP)
        return Some(TimestampOffset(RecordBatch.NO_TIMESTAMP, logStartOffset))
    else if (targetTimestamp == ListOffsetRequest.LATEST_TIMESTAMP)
        return Some(TimestampOffset(RecordBatch.NO_TIMESTAMP, logEndOffset))

    val targetSeg = {
      // Get all the segments whose largest timestamp is smaller than target timestamp
      val earlierSegs = segmentsCopy.takeWhile(_.largestTimestamp < targetTimestamp)
      // We need to search the first segment whose largest timestamp is greater than the target timestamp if there is one.
      if (earlierSegs.length < segmentsCopy.length)
        Some(segmentsCopy(earlierSegs.length))
      else
        None
    }

    targetSeg.flatMap(_.findOffsetByTimestamp(targetTimestamp, logStartOffset))
  }

  /**
   * Given a message offset, find its corresponding offset metadata in the log.
   * If the message offset is out of range, return unknown offset metadata
   */
  def convertToOffsetMetadata(offset: Long): LogOffsetMetadata = {
    try {
      val fetchDataInfo = readUncommitted(offset, 1)
      fetchDataInfo.fetchOffsetMetadata
    } catch {
      case _: OffsetOutOfRangeException => LogOffsetMetadata.UnknownOffsetMetadata
    }
  }

  /**
   * Delete any log segments matching the given predicate function,
   * starting with the oldest segment and moving forward until a segment doesn't match.
   *
   * @param predicate A function that takes in a single log segment and returns true iff it is deletable
   * @return The number of segments deleted
   */
  private def deleteOldSegments(predicate: LogSegment => Boolean): Int = {
    lock synchronized {//加锁
      val deletable = deletableSegments(predicate)
      deleteSegments(deletable)
    }
  }

  private def deleteSegments(deletable: Iterable[LogSegment]): Int = {
    val numToDelete = deletable.size
    if (numToDelete > 0) {
      // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
      if (segments.size == numToDelete)//全部LogSegment都符合删除
        roll()//至少保留一个LogSegment，删除前，创建一个新的activeSegment
      lock synchronized {
        // remove the segments for lookups
        //删除LogSegment
        deletable.foreach(deleteSegment)
        maybeIncrementLogStartOffset(segments.firstEntry.getValue.baseOffset)
      }
    }
    numToDelete
  }

  /**
    * Find segments starting from the oldest until the user-supplied predicate is false.
    * A final segment that is empty will never be returned (since we would just end up re-creating it).
    * @param predicate A function that takes in a single log segment and returns true iff it is deletable
    * @return the segments ready to be deleted
    */
  private def deletableSegments(predicate: LogSegment => Boolean) = {
    //获得activeSegment
    val lastEntry = segments.lastEntry
    if (lastEntry == null) Seq.empty
    //通过LogSegment方法得到segment跳表中的value,之后循环检测logSegment是否满足删除条件
    else logSegments.takeWhile(s => predicate(s) && (s.baseOffset != lastEntry.getValue.baseOffset || s.size > 0))
  }

  /**
    * Delete any log segments that have either expired due to time based retention
    * or because the log size is > retentionSize
    */
  def deleteOldSegments(): Int = {
    if (!config.delete) return 0
    deleteRetentionMsBreachedSegments() + deleteRetentionSizeBreachedSegments() + deleteLogStartOffsetBreachedSegments()
  }

  //根据LogSegment存活时长判断是否要删除LogSegment
  private def deleteRetentionMsBreachedSegments() : Int = {
    if (config.retentionMs < 0) return 0
    val startMs = time.milliseconds
    //LogManager直接管理Log，但不能直接管理LogSegment，所以删除LogSegment的任务交给log
    //删除条件是LogSegment在一端时间（retentionMs）内没有没修改
    deleteOldSegments(startMs - _.largestTimestamp > config.retentionMs)
  }

  //根据retention.bytes配置项的值和当前log的大小判断是否删除LogSegment
  private def deleteRetentionSizeBreachedSegments() : Int = {
    if (config.retentionSize < 0 || size < config.retentionSize) return 0\
    //计算要删除的字节数
    var diff = size - config.retentionSize
    //判断是不是需要删除
    def shouldDelete(segment: LogSegment) = {
      if (diff - segment.size >= 0) {
        diff -= segment.size
        true
      } else {
        false
      }
    }
    //根据执行结果进行删除
    deleteOldSegments(shouldDelete)
  }

  private def deleteLogStartOffsetBreachedSegments() : Int = {
    // keep active segment to avoid frequent log rolling due to user's DeleteRecordsRequest
    lock synchronized {
      val deletable = {
        if (segments.size() < 2)
          Seq.empty
        else
          logSegments.sliding(2).takeWhile { iterable =>
            val nextSegment = iterable.toSeq(1)
            nextSegment.baseOffset <= logStartOffset
          }.map(_.toSeq(0)).toSeq
      }
      deleteSegments(deletable)
    }
  }

  /**
   * The size of the log in bytes
   */
  def size: Long = logSegments.map(_.size).sum

  /**
   * The offset metadata of the next message that will be appended to the log
   */
  def logEndOffsetMetadata: LogOffsetMetadata = nextOffsetMetadata

  /**
   * The offset of the next message that will be appended to the log
   */
  def logEndOffset: Long = nextOffsetMetadata.messageOffset

  /**
   * Roll the log over to a new empty log segment if necessary.
   *
   * @param messagesSize The messages set size in bytes
   * @param maxTimestampInMessages The maximum timestamp in the messages.
   * logSegment will be rolled if one of the following conditions met
   * <ol>
   * <li> The logSegment is full
   * <li> The maxTime has elapsed since the timestamp of first message in the segment (or since the create time if
   * the first message does not have a timestamp)
   * <li> The index is full
   * </ol>
   * @return The currently active segment after (perhaps) rolling to a new segment
   */
  //当前activeSegment的日志大小加上上次待追加的消息大小超过Logsegment的最大长度
  //当前activeSegment的寿命超过了LogSegment配置的最长存活时间
  //索引文件满了
  private def maybeRoll(messagesSize: Int, maxTimestampInMessages: Long, maxOffsetInMessages: Long): LogSegment = {
    val segment = activeSegment
    val now = time.milliseconds
    val reachedRollMs = segment.timeWaitedForRoll(now, maxTimestampInMessages) > config.segmentMs - segment.rollJitterMs
    if (segment.size > config.segmentSize - messagesSize ||
        (segment.size > 0 && reachedRollMs) ||
        segment.index.isFull || segment.timeIndex.isFull || !segment.canConvertToRelativeOffset(maxOffsetInMessages)) {
      debug(s"Rolling new log segment in $name (log_size = ${segment.size}/${config.segmentSize}}, " +
          s"index_size = ${segment.index.entries}/${segment.index.maxEntries}, " +
          s"time_index_size = ${segment.timeIndex.entries}/${segment.timeIndex.maxEntries}, " +
          s"inactive_time_ms = ${segment.timeWaitedForRoll(now, maxTimestampInMessages)}/${config.segmentMs - segment.rollJitterMs}).")
      /*
        maxOffsetInMessages - Integer.MAX_VALUE is a heuristic value for the first offset in the set of messages.
        Since the offset in messages will not differ by more than Integer.MAX_VALUE, this is guaranteed <= the real
        first offset in the set. Determining the true first offset in the set requires decompression, which the follower
        is trying to avoid during log append. Prior behavior assigned new baseOffset = logEndOffset from old segment.
        This was problematic in the case that two consecutive messages differed in offset by
        Integer.MAX_VALUE.toLong + 2 or more.  In this case, the prior behavior would roll a new log segment whose
        base offset was too low to contain the next message.  This edge case is possible when a replica is recovering a
        highly compacted topic from scratch.
       */
      //创建新的segment
      roll(maxOffsetInMessages - Integer.MAX_VALUE)
    } else {
      segment
    }
  }

  /**
   * Roll the log over to a new active segment starting with the current logEndOffset.
   * This will trim the index to the exact size of the number of entries it currently contains.
   *
   * @return The newly rolled segment
   */
  def roll(expectedNextOffset: Long = 0): LogSegment = {
    val start = time.nanoseconds
    lock synchronized {
      //获取LEO
      val newOffset = math.max(expectedNextOffset, logEndOffset)
      val logFile = Log.logFile(dir, newOffset)
      val offsetIdxFile = offsetIndexFile(dir, newOffset)
      val timeIdxFile = timeIndexFile(dir, newOffset)
      val txnIdxFile = transactionIndexFile(dir, newOffset)
      //输出警告信息
      for(file <- List(logFile, offsetIdxFile, timeIdxFile, txnIdxFile) if file.exists) {
        warn("Newly rolled segment file " + file.getName + " already exists; deleting it first")
        file.delete()
      }

      segments.lastEntry() match {
        case null =>
        case entry => {
          val seg = entry.getValue
          seg.onBecomeInactiveSegment()
          //对索引进行截断
          seg.index.trimToValidSize()
          seg.timeIndex.trimToValidSize()
          //对日志进行截断
          seg.log.trim()
        }
      }

      // take a snapshot of the producer state to facilitate recovery. It is useful to have the snapshot
      // offset align with the new segment offset since this ensures we can recover the segment by beginning
      // with the corresponding snapshot file and scanning the segment data. Because the segment base offset
      // may actually be ahead of the current producer state end offset (which corresponds to the log end offset),
      // we manually override the state offset here prior to taking the snapshot.
      producerStateManager.updateMapEndOffset(newOffset)
      producerStateManager.takeSnapshot()
      //创建新的Segment
      val segment = new LogSegment(dir,
                                   startOffset = newOffset,
                                   indexIntervalBytes = config.indexInterval,
                                   maxIndexSize = config.maxIndexSize,
                                   rollJitterMs = config.randomSegmentJitter,
                                   time = time,
                                   fileAlreadyExists = false,
                                   initFileSize = initFileSize,
                                   preallocate = config.preallocate)
      //将新创建的segment添加到跳表中
      val prev = addSegment(segment)
      if(prev != null)
        throw new KafkaException("Trying to roll a new log segment for topic partition %s with start offset %d while it already exists.".format(name, newOffset))

      // We need to update the segment base offset and append position data of the metadata when log rolls.
      // The next offset should not change.
      //更新baseOffset和activiSegment.size
      updateLogEndOffset(nextOffsetMetadata.messageOffset)

      // schedule an asynchronous flush of the old segment
      //执行flush操作，使用kafkascheduler线程池执行flush()方法
      scheduler.schedule("flush-log", () => flush(newOffset), delay = 0L)

      info("Rolled new log segment for '" + name + "' in %.0f ms.".format((System.nanoTime - start) / (1000.0*1000.0)))
      //返回
      segment
    }
  }

  /**
   * The number of messages appended to the log since the last flush
   */
  def unflushedMessages() = this.logEndOffset - this.recoveryPoint

  /**
   * Flush all log segments
   */
  def flush(): Unit = flush(this.logEndOffset)

  /**
   * Flush log segments for all offsets up to offset-1
   *
   * @param offset The offset to flush up to (non-inclusive); the new recovery point
   */
  def flush(offset: Long) : Unit = {
    //offset之前的消息已经全部刷新到磁盘，不需要刷新
    if (offset <= this.recoveryPoint)
      return
    debug("Flushing log '" + name + " up to offset " + offset + ", last flushed: " + lastFlushTime + " current time: " +
          time.milliseconds + " unflushed = " + unflushedMessages)
    //logSegments 通过跳表，找到recoverPoint和offset之间的LogSegment对象
    for(segment <- logSegments(this.recoveryPoint, offset))
      //调用LogSegment的flush方法会调用日志文件和索引文件的flush方法
      //最终调用系统的fsync命令刷新磁盘，保持数据持久性
      segment.flush()

    // now that we have flushed, we can cleanup old producer snapshots. However, it is useful to retain
    // the snapshots from the recent segments in case we need to truncate and rebuild the producer state.
    // Otherwise, we would always need to rebuild from the earliest segment.
    producerStateManager.deleteSnapshotsBefore(minSnapshotOffsetToRetain(offset))

    lock synchronized {
      if(offset > this.recoveryPoint) {
        this.recoveryPoint = offset
        lastflushedTime.set(time.milliseconds)
      }
    }
  }

  def minSnapshotOffsetToRetain(flushedOffset: Long) = {
    // always retain the producer snapshot from the last two segments. This solves the common case
    // of truncating to an offset within the active segment, and the rarer case of truncating to the
    // previous segment just after rolling the new segment.
    var minSnapshotOffset = activeSegment.baseOffset
    val previousSegment = segments.lowerEntry(activeSegment.baseOffset)
    if (previousSegment != null)
      minSnapshotOffset = previousSegment.getValue.baseOffset
    math.min(flushedOffset, minSnapshotOffset)
  }

  /**
   * Completely delete this log directory and all contents from the file system with no delay
   */
  private[log] def delete() {
    lock synchronized {
      logSegments.foreach(_.delete())
      segments.clear()
      leaderEpochCache.clear()
      Utils.delete(dir)
    }
  }

  // visible for testing
  private[log] def takeProducerSnapshot(): Unit = lock synchronized {
    producerStateManager.takeSnapshot()
  }

  // visible for testing
  private[log] def latestProducerSnapshotOffset: Option[Long] = lock synchronized {
    producerStateManager.latestSnapshotOffset
  }

  // visible for testing
  private[log] def oldestProducerSnapshotOffset: Option[Long] = lock synchronized {
    producerStateManager.oldestSnapshotOffset
  }

  // visible for testing
  private[log] def latestProducerStateEndOffset: Long = lock synchronized {
    producerStateManager.mapEndOffset
  }

  /**
   * Truncate this log so that it ends with the greatest offset < targetOffset.
   *
   * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
   */
  private[log] def truncateTo(targetOffset: Long) {
    if(targetOffset < 0)
      throw new IllegalArgumentException("Cannot truncate to a negative offset (%d).".format(targetOffset))
    if(targetOffset >= logEndOffset) {
      info("Truncating %s to %d has no effect as the largest offset in the log is %d.".format(name, targetOffset, logEndOffset-1))
      return
    }
    info("Truncating log %s to offset %d.".format(name, targetOffset))
    lock synchronized {
      if(segments.firstEntry.getValue.baseOffset > targetOffset) {
        truncateFullyAndStartAt(targetOffset)
      } else {
        val deletable = logSegments.filter(segment => segment.baseOffset > targetOffset)
        deletable.foreach(deleteSegment)
        activeSegment.truncateTo(targetOffset)
        updateLogEndOffset(targetOffset)
        this.recoveryPoint = math.min(targetOffset, this.recoveryPoint)
        this.logStartOffset = math.min(targetOffset, this.logStartOffset)
        leaderEpochCache.clearAndFlushLatest(targetOffset)
        loadProducerState(targetOffset, reloadFromCleanShutdown = false)
      }
    }
  }

  /**
   *  Delete all data in the log and start at the new offset
   *
   *  @param newOffset The new offset to start the log with
   */
  private[log] def truncateFullyAndStartAt(newOffset: Long) {
    debug(s"Truncate and start log '$name' at offset $newOffset")
    lock synchronized {
      val segmentsToDelete = logSegments.toList
      segmentsToDelete.foreach(deleteSegment)
      addSegment(new LogSegment(dir,
                                newOffset,
                                indexIntervalBytes = config.indexInterval,
                                maxIndexSize = config.maxIndexSize,
                                rollJitterMs = config.randomSegmentJitter,
                                time = time,
                                fileAlreadyExists = false,
                                initFileSize = initFileSize,
                                preallocate = config.preallocate))
      updateLogEndOffset(newOffset)
      leaderEpochCache.clearAndFlush()

      producerStateManager.truncate()
      producerStateManager.updateMapEndOffset(newOffset)
      updateFirstUnstableOffset()

      this.recoveryPoint = math.min(newOffset, this.recoveryPoint)
      this.logStartOffset = newOffset
    }
  }

  /**
   * The time this log is last known to have been fully flushed to disk
   */
  def lastFlushTime(): Long = lastflushedTime.get

  /**
   * The active segment that is currently taking appends
   */
  def activeSegment = segments.lastEntry.getValue

  /**
   * All the log segments in this log ordered from oldest to newest
   */
  def logSegments: Iterable[LogSegment] = segments.values.asScala

  /**
   * Get all segments beginning with the segment that includes "from" and ending with the segment
   * that includes up to "to-1" or the end of the log (if to > logEndOffset)
   */
  def logSegments(from: Long, to: Long): Iterable[LogSegment] = {
    lock synchronized {
      val floor = segments.floorKey(from)
      if(floor eq null)
        segments.headMap(to).values.asScala
      else
        segments.subMap(floor, true, to, false).values.asScala
    }
  }

  override def toString = "Log(" + dir + ")"

  /**
   * This method performs an asynchronous log segment delete by doing the following:
   * <ol>
   *   <li>It removes the segment from the segment map so that it will no longer be used for reads.
   *   <li>It renames the index and log files by appending .deleted to the respective file name
   *   <li>It schedules an asynchronous delete operation to occur in the future
   * </ol>
   * This allows reads to happen concurrently without synchronization and without the possibility of physically
   * deleting a file while it is being read from.
   *
   * @param segment The log segment to schedule for deletion
   */
  private def deleteSegment(segment: LogSegment) {
    info("Scheduling log segment %d for log %s for deletion.".format(segment.baseOffset, name))
    lock synchronized {
      //从segments中删除LogSegment对象
      segments.remove(segment.baseOffset)
      //异步的删除LogSegment对应的日志文件和索引文件
      asyncDeleteSegment(segment)
    }
  }

  /**
   * Perform an asynchronous delete on the given file if it exists (otherwise do nothing)
   *
   * @throws KafkaStorageException if the file can't be renamed and still exists
   */
  private def asyncDeleteSegment(segment: LogSegment) {
    //将文件后缀改为 .deleted
    segment.changeFileSuffixes("", Log.DeletedFileSuffix)
    def deleteSeg() {
      info("Deleting segment %d from log %s.".format(segment.baseOffset, name))
      //删除日志文件和索引文件
      segment.delete()
    }
    //将deleteSeg方法作为定时任务放入scheduler线程池中等待执行
    scheduler.schedule("delete-file", deleteSeg _, delay = config.fileDeleteDelayMs)
  }

  /**
   * Swap a new segment in place and delete one or more existing segments in a crash-safe manner. The old segments will
   * be asynchronously deleted.
   *
   * The sequence of operations is:
   * <ol>
   *   <li> Cleaner creates new segment with suffix .cleaned and invokes replaceSegments().
   *        If broker crashes at this point, the clean-and-swap operation is aborted and
   *        the .cleaned file is deleted on recovery in loadSegments().
   *   <li> New segment is renamed .swap. If the broker crashes after this point before the whole
   *        operation is completed, the swap operation is resumed on recovery as described in the next step.
   *   <li> Old segment files are renamed to .deleted and asynchronous delete is scheduled.
   *        If the broker crashes, any .deleted files left behind are deleted on recovery in loadSegments().
   *        replaceSegments() is then invoked to complete the swap with newSegment recreated from
   *        the .swap file and oldSegments containing segments which were not renamed before the crash.
   *   <li> Swap segment is renamed to replace the existing segment, completing this operation.
   *        If the broker crashes, any .deleted files which may be left behind are deleted
   *        on recovery in loadSegments().
   * </ol>
   *
   * @param newSegment The new log segment to add to the log
   * @param oldSegments The old log segments to delete from the log
   * @param isRecoveredSwapFile true if the new segment was created from a swap file during recovery after a crash
   */
  private[log] def replaceSegments(newSegment: LogSegment, oldSegments: Seq[LogSegment], isRecoveredSwapFile : Boolean = false) {
    lock synchronized {
      // need to do this in two phases to be crash safe AND do the delete asynchronously
      // if we crash in the middle of this we complete the swap in loadSegments()
      if (!isRecoveredSwapFile)
        newSegment.changeFileSuffixes(Log.CleanedFileSuffix, Log.SwapFileSuffix)
      addSegment(newSegment)

      // delete the old files
      for(seg <- oldSegments) {
        // remove the index entry
        if(seg.baseOffset != newSegment.baseOffset)
          segments.remove(seg.baseOffset)
        // delete segment
        asyncDeleteSegment(seg)
      }
      // okay we are safe now, remove the swap suffix
      newSegment.changeFileSuffixes(Log.SwapFileSuffix, "")
    }
  }

  /**
   * remove deleted log metrics
   */
  private[log] def removeLogMetrics(): Unit = {
    removeMetric("NumLogSegments", tags)
    removeMetric("LogStartOffset", tags)
    removeMetric("LogEndOffset", tags)
    removeMetric("Size", tags)
  }
  /**
   * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
   *
   * @param segment The segment to add
   */
  def addSegment(segment: LogSegment) = this.segments.put(segment.baseOffset, segment)

}

/**
 * Helper functions for logs
 */
object Log {

  /** a log file */
  val LogFileSuffix = ".log"

  /** an index file */
  val IndexFileSuffix = ".index"

  /** a time index file */
  val TimeIndexFileSuffix = ".timeindex"

  val PidSnapshotFileSuffix = ".snapshot"

  /** an (aborted) txn index */
  val TxnIndexFileSuffix = ".txnindex"

  /** a file that is scheduled to be deleted */
  val DeletedFileSuffix = ".deleted"

  /** A temporary file that is being used for log cleaning */
  val CleanedFileSuffix = ".cleaned"

  /** A temporary file used when swapping files into the log */
  val SwapFileSuffix = ".swap"

  /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8. This is required to maintain backwards compatibility
   * with 0.8 and avoid unnecessary log recovery when upgrading from 0.8 to 0.8.1 */
  /** TODO: Get rid of CleanShutdownFile in 0.8.2 */
  val CleanShutdownFile = ".kafka_cleanshutdown"

  /** a directory that is scheduled to be deleted */
  val DeleteDirSuffix = "-delete"

  private val DeleteDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$DeleteDirSuffix")

  val UnknownLogStartOffset = -1L

  def apply(dir: File,
            config: LogConfig,
            logStartOffset: Long = 0L,
            recoveryPoint: Long = 0L,
            scheduler: Scheduler,
            brokerTopicStats: BrokerTopicStats,
            time: Time = Time.SYSTEM,
            maxProducerIdExpirationMs: Int = 60 * 60 * 1000,
            producerIdExpirationCheckIntervalMs: Int = 10 * 60 * 1000): Log = {
    val topicPartition = Log.parseTopicPartitionName(dir)
    val producerStateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)
    new Log(dir, config, logStartOffset, recoveryPoint, scheduler, brokerTopicStats, time, maxProducerIdExpirationMs,
      producerIdExpirationCheckIntervalMs, topicPartition, producerStateManager)
  }

  /**
   * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
   * so that ls sorts the files numerically.
   *
   * @param offset The offset to use in the file name
   * @return The filename
   */
  def filenamePrefixFromOffset(offset: Long): String = {
    val nf = NumberFormat.getInstance()
    nf.setMinimumIntegerDigits(20)
    nf.setMaximumFractionDigits(0)
    nf.setGroupingUsed(false)
    nf.format(offset)
  }

  /**
   * Construct a log file name in the given dir with the given base offset
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def logFile(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix)

  /**
    * Return a directory name to rename the log directory to for async deletion. The name will be in the following
    * format: topic-partition.uniqueId-delete where topic, partition and uniqueId are variables.
    */
  def logDeleteDirName(logName: String): String = {
    val uniqueId = java.util.UUID.randomUUID.toString.replaceAll("-", "")
    s"$logName.$uniqueId$DeleteDirSuffix"
  }

  /**
   * Construct an index file name in the given dir using the given base offset
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def offsetIndexFile(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix)

  /**
   * Construct a time index file name in the given dir using the given base offset
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def timeIndexFile(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + TimeIndexFileSuffix)

  /**
   * Construct a producer id snapshot file using the given offset.
   *
   * @param dir The directory in which the log will reside
   * @param offset The last offset (exclusive) included in the snapshot
   */
  def producerSnapshotFile(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + PidSnapshotFileSuffix)

  def transactionIndexFile(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + TxnIndexFileSuffix)

  def offsetFromFilename(filename: String): Long =
    filename.substring(0, filename.indexOf('.')).toLong

  /**
   * Parse the topic and partition out of the directory name of a log
   */
  def parseTopicPartitionName(dir: File): TopicPartition = {
    if (dir == null)
      throw new KafkaException("dir should not be null")

    def exception(dir: File): KafkaException = {
      new KafkaException(s"Found directory ${dir.getCanonicalPath}, '${dir.getName}' is not in the form of " +
        "topic-partition or topic-partition.uniqueId-delete (if marked for deletion).\n" +
        "Kafka's log directories (and children) should only contain Kafka topic data.")
    }

    val dirName = dir.getName
    if (dirName == null || dirName.isEmpty || !dirName.contains('-'))
      throw exception(dir)
    if (dirName.endsWith(DeleteDirSuffix) && !DeleteDirPattern.matcher(dirName).matches)
      throw exception(dir)

    val name: String =
      if (dirName.endsWith(DeleteDirSuffix)) dirName.substring(0, dirName.lastIndexOf('.'))
      else dirName

    val index = name.lastIndexOf('-')
    val topic = name.substring(0, index)
    val partitionString = name.substring(index + 1)
    if (topic.isEmpty || partitionString.isEmpty)
      throw exception(dir)

    val partition =
      try partitionString.toInt
      catch { case _: NumberFormatException => throw exception(dir) }

    new TopicPartition(topic, partition)
  }

  private def isIndexFile(file: File): Boolean = {
    val filename = file.getName
    filename.endsWith(IndexFileSuffix) || filename.endsWith(TimeIndexFileSuffix) || filename.endsWith(TxnIndexFileSuffix)
  }

  private def isLogFile(file: File): Boolean =
    file.getPath.endsWith(LogFileSuffix)

}
