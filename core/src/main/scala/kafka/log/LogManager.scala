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

import java.io._
import java.nio.file.Files
import java.util.concurrent._

import kafka.admin.AdminUtils
import kafka.common.{KafkaException, KafkaStorageException}
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.server.{BrokerState, RecoveringFromUncleanShutdown, _}
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection._

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 * 
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 * 
 * A background thread handles log retention by periodically truncating excess log segments.
 */
@threadsafe
//用来管理一个broker上所有的log
//启动了三个后台任务
//log-flusher 日志刷写
//log-retention 日志保留
//recovery-point-checkpoint 检查点刷新
//Cleaner线程 日志清理
class LogManager(val logDirs: Array[File],//目录集合，在配置文件中可以通过log.dirs指定多个目录，在创建时，会选择log最少得log目录创建log
                 val topicConfigs: Map[String, LogConfig], // note that this doesn't get updated after creation
                 val defaultConfig: LogConfig,
                 val cleanerConfig: CleanerConfig,
                 ioThreads: Int,//加载log
                 val flushCheckMs: Long,
                 val flushRecoveryOffsetCheckpointMs: Long,
                 val flushStartOffsetCheckpointMs: Long,
                 val retentionCheckMs: Long,
                 val maxPidExpirationMs: Int,
                 scheduler: Scheduler,//周期执行任务的线程池
                 val brokerState: BrokerState,
                 brokerTopicStats: BrokerTopicStats,
                 time: Time) extends Logging {
  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  val LogStartOffsetCheckpointFile = "log-start-offset-checkpoint"
  val LockFile = ".lock"
  val InitialTaskDelayMs = 30*1000

  private val logCreationOrDeletionLock = new Object
  private val logs = new Pool[TopicPartition, Log]()//用来管理TopicAndPartition与log之间的对应关系
  private val logsToBeDeleted = new LinkedBlockingQueue[Log]()

  createAndValidateLogDirs(logDirs)
  private val dirLocks = lockLogDirs(logDirs)//FileLock集合，为每个log目录加文件锁
  private val recoveryPointCheckpoints = logDirs.map(dir => (dir, new OffsetCheckpointFile(new File(dir, RecoveryPointCheckpointFile)))).toMap
  private val logStartOffsetCheckpoints = logDirs.map(dir => (dir, new OffsetCheckpointFile(new File(dir, LogStartOffsetCheckpointFile)))).toMap
  //创建log放入logs中
  loadLogs()//

  // public, so we can access this from kafka.admin.DeleteTopicTest
  val cleaner: LogCleaner =
    if(cleanerConfig.enableCleaner)
      new LogCleaner(cleanerConfig, logDirs, logs, time = time)
    else
      null
  
  /**
   * Create and check validity of the given directories, specifically:
   * <ol>
   * <li> Ensure that there are no duplicates in the directory list
   * <li> Create each directory if it doesn't exist
   * <li> Check that each path is a readable directory 
   * </ol>
   */
  private def createAndValidateLogDirs(dirs: Seq[File]) {
    if(dirs.map(_.getCanonicalPath).toSet.size < dirs.size)
      throw new KafkaException("Duplicate log directory found: " + logDirs.mkString(", "))
    for(dir <- dirs) {
      if(!dir.exists) {
        info("Log directory '" + dir.getAbsolutePath + "' not found, creating it.")
        val created = dir.mkdirs()
        if(!created)
          throw new KafkaException("Failed to create data directory " + dir.getAbsolutePath)
      }
      if(!dir.isDirectory || !dir.canRead)
        throw new KafkaException(dir.getAbsolutePath + " is not a readable log directory.")
    }
  }
  
  /**
   * Lock all the given directories
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    dirs.map { dir =>
      val lock = new FileLock(new File(dir, LockFile))
      if(!lock.tryLock())
        throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParentFile.getAbsolutePath + 
                               ". A Kafka instance in another process or thread is using this directory.")
      lock
    }
  }
  
  /**
   * Recover and load all logs in the given data directories
   */
  //加载log目录下的所有log
  private def loadLogs(): Unit = {
    info("Loading logs.")
    val startMs = time.milliseconds
    val threadPools = mutable.ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]
    //为每个log分配一个有ioThreads条线程的线程池，用来执行恢复操作
    for (dir <- this.logDirs) {
      val pool = Executors.newFixedThreadPool(ioThreads)
      threadPools.append(pool)

      //检测上次关闭是否正常
      val cleanShutdownFile = new File(dir, Log.CleanShutdownFile)
      if (cleanShutdownFile.exists) {
        debug(
          "Found clean shutdown file. " +
          "Skipping recovery for all logs in data directory: " +
          dir.getAbsolutePath)
      } else {
        // log recovery itself is being performed by `Log` class during initialization
        brokerState.newState(RecoveringFromUncleanShutdown)
      }

      //载入每个线程的recoveryPoints
      var recoveryPoints = Map[TopicPartition, Long]()
      try {
        recoveryPoints = this.recoveryPointCheckpoints(dir).read
      } catch {
        case e: Exception =>
          warn("Error occurred while reading recovery-point-offset-checkpoint file of directory " + dir, e)
          warn("Resetting the recovery checkpoint to 0")
      }

      var logStartOffsets = Map[TopicPartition, Long]()
      try {
        logStartOffsets = this.logStartOffsetCheckpoints(dir).read
      } catch {
        case e: Exception =>
          warn("Error occurred while reading log-start-offset-checkpoint file of directory " + dir, e)
      }

      //遍历所有的log目录的子文件，将文件过滤掉，只保留目录
      val jobsForDir = for {
        dirContent <- Option(dir.listFiles).toList
        logDir <- dirContent if logDir.isDirectory
      } yield {
        CoreUtils.runnable {
          debug("Loading log '" + logDir.getName + "'")
          //目录中解析出topic和分区编号
          val topicPartition = Log.parseTopicPartitionName(logDir)
          //获取log配置
          val config = topicConfigs.getOrElse(topicPartition.topic, defaultConfig)
          //获取log对应的recoveryPoint
          val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)
          val logStartOffset = logStartOffsets.getOrElse(topicPartition, 0L)
          //创建log对象
          val current = Log(
            dir = logDir,
            config = config,
            logStartOffset = logStartOffset,
            recoveryPoint = logRecoveryPoint,
            maxProducerIdExpirationMs = maxPidExpirationMs,
            scheduler = scheduler,
            time = time,
            brokerTopicStats = brokerTopicStats)
          if (logDir.getName.endsWith(Log.DeleteDirSuffix)) {
            this.logsToBeDeleted.add(current)
          } else {
            //将log对象保存到logs集合中
            val previous = this.logs.put(topicPartition, current)
            if (previous != null) {
              throw new IllegalArgumentException(
                "Duplicate log directories found: %s, %s!".format(
                  current.dir.getAbsolutePath, previous.dir.getAbsolutePath))
            }
          }
        }
      }
      //将jobsForDir中所有的任务放到线程池中执行，并将future形成seq.保存到jobs中
      jobs(cleanShutdownFile) = jobsForDir.map(pool.submit)
    }


    try {
      //等待jobs中任务完成
      for ((cleanShutdownFile, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)
        cleanShutdownFile.delete()
      }
    } catch {
      case e: ExecutionException => {
        error("There was an error in one of the threads during logs loading: " + e.getCause)
        throw e.getCause
      }
    } finally {
      //循环关闭线程池
      threadPools.foreach(_.shutdown())
    }

    info(s"Logs loading complete in ${time.milliseconds - startMs} ms.")
  }

  /**
   *  Start the background threads to flush logs and do log cleanup
   */
  def startup() {
    /* Schedule the cleanup task to delete old logs */
    if(scheduler != null) {
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      //启动log retention任务 cleanupLogs
      scheduler.schedule("kafka-log-retention",
                         cleanupLogs _,
                         delay = InitialTaskDelayMs,
                         period = retentionCheckMs,
                         TimeUnit.MILLISECONDS)
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      //启动log flusher任务 flushDirtyLogs
      scheduler.schedule("kafka-log-flusher",
                         flushDirtyLogs _,
                         delay = InitialTaskDelayMs,
                         period = flushCheckMs,
                         TimeUnit.MILLISECONDS)
      //启动recovery point checkpoint任务 checkpointRecoveryPointOffsets
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         checkpointRecoveryPointOffsets _,
                         delay = InitialTaskDelayMs,
                         period = flushRecoveryOffsetCheckpointMs,
                         TimeUnit.MILLISECONDS)
      scheduler.schedule("kafka-log-start-offset-checkpoint",
                         checkpointLogStartOffsets _,
                         delay = InitialTaskDelayMs,
                         period = flushStartOffsetCheckpointMs,
                         TimeUnit.MILLISECONDS)
      //启动log delete任务
      scheduler.schedule("kafka-delete-logs",
                         deleteLogs _,
                         delay = InitialTaskDelayMs,
                         period = defaultConfig.fileDeleteDelayMs,
                         TimeUnit.MILLISECONDS)
    }
    //根据log配置是否启动LogCleaner  进行压缩
    if(cleanerConfig.enableCleaner)
      cleaner.startup()
  }

  /**
   * Close all the logs
   */
  def shutdown() {
    info("Shutting down.")

    val threadPools = mutable.ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // stop the cleaner first
    if (cleaner != null) {
      CoreUtils.swallow(cleaner.shutdown())
    }

    // close logs in each dir
    for (dir <- this.logDirs) {
      debug("Flushing and closing logs at " + dir)

      val pool = Executors.newFixedThreadPool(ioThreads)
      threadPools.append(pool)

      val logsInDir = logsByDir.getOrElse(dir.toString, Map()).values

      val jobsForDir = logsInDir map { log =>
        CoreUtils.runnable {
          // flush the log to ensure latest possible recovery point
          log.flush()
          log.close()
        }
      }

      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }

    try {
      for ((dir, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)

        // update the last flush point
        debug("Updating recovery points at " + dir)
        checkpointLogRecoveryOffsetsInDir(dir)

        debug("Updating log start offsets at " + dir)
        checkpointLogStartOffsetsInDir(dir)

        // mark that the shutdown was clean by creating marker file
        debug("Writing clean shutdown marker at " + dir)
        CoreUtils.swallow(Files.createFile(new File(dir, Log.CleanShutdownFile).toPath))
      }
    } catch {
      case e: ExecutionException => {
        error("There was an error in one of the threads during LogManager shutdown: " + e.getCause)
        throw e.getCause
      }
    } finally {
      threadPools.foreach(_.shutdown())
      // regardless of whether the close succeeded, we need to unlock the data directories
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.")
  }

  /**
   * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
   *
   * @param partitionOffsets Partition logs that need to be truncated
   */
  def truncateTo(partitionOffsets: Map[TopicPartition, Long]) {
    for ((topicPartition, truncateOffset) <- partitionOffsets) {
      val log = logs.get(topicPartition)
      // If the log does not exist, skip it
      if (log != null) {
        //May need to abort and pause the cleaning of the log, and resume after truncation is done.
        val needToStopCleaner = cleaner != null && truncateOffset < log.activeSegment.baseOffset
        if (needToStopCleaner)
          cleaner.abortAndPauseCleaning(topicPartition)
        try {
          log.truncateTo(truncateOffset)
          if (needToStopCleaner)
            cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicPartition, log.activeSegment.baseOffset)
        } finally {
          if (needToStopCleaner)
            cleaner.resumeCleaning(topicPartition)
        }
      }
    }
    checkpointRecoveryPointOffsets()
  }

  /**
   *  Delete all data in a partition and start the log at the new offset
   *  @param newOffset The new offset to start the log with
   */
  def truncateFullyAndStartAt(topicPartition: TopicPartition, newOffset: Long) {
    val log = logs.get(topicPartition)
    // If the log does not exist, skip it
    if (log != null) {
        //Abort and pause the cleaning of the log, and resume after truncation is done.
      if (cleaner != null)
        cleaner.abortAndPauseCleaning(topicPartition)
      log.truncateFullyAndStartAt(newOffset)
      if (cleaner != null) {
        cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicPartition, log.activeSegment.baseOffset)
        cleaner.resumeCleaning(topicPartition)
      }
    }
    checkpointRecoveryPointOffsets()
  }

  /**
   * Write out the current recovery point for all logs to a text file in the log directory 
   * to avoid recovering the whole log on startup.
   */
  //会周期的调用方法完成RecoveryPointCheckpoint文件的更新
  //RecoveryPointCheckpoint文件，其中记录了log目录下每个log的recoveryPoint值
  //此文件会在broker启动时帮助Broker进行log的恢复工作
  def checkpointRecoveryPointOffsets() {
    this.logDirs.foreach(checkpointLogRecoveryOffsetsInDir)
  }

  /**
   * Write out the current log start offset for all logs to a text file in the log directory
   * to avoid exposing data that have been deleted by DeleteRecordsRequest
   */
  def checkpointLogStartOffsets() {
    this.logDirs.foreach(checkpointLogStartOffsetsInDir)
  }

  /**
   * Make a checkpoint for all logs in provided directory.
   */
  //定期的将每个log的recoveryPoint写入RecoveryPointCheckpoint文件中
  private def checkpointLogRecoveryOffsetsInDir(dir: File): Unit = {
    //获得指定目录下的TopicAndPartiton信息，已经对应的Log对象
    val recoveryPoints = this.logsByDir.get(dir.toString)
    if (recoveryPoints.isDefined) {
      //更新指定目录下的RevoceryPointCheckpoint文件
      this.recoveryPointCheckpoints(dir).write(recoveryPoints.get.mapValues(_.recoveryPoint))
    }
  }

  /**
   * Checkpoint log start offset for all logs in provided directory.
   */
  private def checkpointLogStartOffsetsInDir(dir: File): Unit = {
    val logs = this.logsByDir.get(dir.toString)
    if (logs.isDefined) {
      this.logStartOffsetCheckpoints(dir).write(
        logs.get.filter{case (tp, log) => log.logStartOffset > log.logSegments.head.baseOffset}.mapValues(_.logStartOffset))
    }
  }

  /**
   * Get the log if it exists, otherwise return None
   */
  def getLog(topicPartition: TopicPartition): Option[Log] = Option(logs.get(topicPartition))

  /**
   * Create a log for the given topic and the given partition
   * If the log already exists, just return a copy of the existing log
   */
  //在选择Log的log目录时，会选择Log最少得log目录
  def createLog(topicPartition: TopicPartition, config: LogConfig): Log = {
    logCreationOrDeletionLock synchronized {
      // create the log if it has not already been created in another thread
      getLog(topicPartition).getOrElse {
        //现则log最少得log目录
        val dataDir = nextLogDir()
        val dir = new File(dataDir, topicPartition.topic + "-" + topicPartition.partition)
        Files.createDirectories(dir.toPath)
        //创建log对象
        val log = Log(
          dir = dir,
          config = config,
          logStartOffset = 0L,
          recoveryPoint = 0L,
          maxProducerIdExpirationMs = maxPidExpirationMs,
          scheduler = scheduler,
          time = time,
          brokerTopicStats = brokerTopicStats)
        //存入log集合中
        logs.put(topicPartition, log)
        info("Created log for partition [%s,%d] in %s with properties {%s}."
          .format(topicPartition.topic,
            topicPartition.partition,
            dataDir.getAbsolutePath,
            config.originals.asScala.mkString(", ")))
        log
      }
    }
  }

  /**
   *  Delete logs marked for deletion.
   */
  private def deleteLogs(): Unit = {
    try {
      var failed = 0
      while (!logsToBeDeleted.isEmpty && failed < logsToBeDeleted.size()) {
        val removedLog = logsToBeDeleted.take()
        if (removedLog != null) {
          try {
            //删除
            removedLog.delete()
            info(s"Deleted log for partition ${removedLog.topicPartition} in ${removedLog.dir.getAbsolutePath}.")
          } catch {
            case e: Throwable =>
              error(s"Exception in deleting $removedLog. Moving it to the end of the queue.", e)
              failed = failed + 1
              logsToBeDeleted.put(removedLog)
          }
        }
      }
    } catch {
      case e: Throwable => 
        error(s"Exception in kafka-delete-logs thread.", e)
    }
  }

  /**
    * Rename the directory of the given topic-partition "logdir" as "logdir.uuid.delete" and 
    * add it in the queue for deletion. 
    * @param topicPartition TopicPartition that needs to be deleted
    */
  def asyncDelete(topicPartition: TopicPartition) = {
    val removedLog: Log = logCreationOrDeletionLock synchronized {
      logs.remove(topicPartition)
    }
    if (removedLog != null) {
      //We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
      if (cleaner != null) {
        cleaner.abortCleaning(topicPartition)
        cleaner.updateCheckpoints(removedLog.dir.getParentFile)
      }
      val dirName = Log.logDeleteDirName(removedLog.name)
      removedLog.close()
      val renamedDir = new File(removedLog.dir.getParent, dirName)
      val renameSuccessful = removedLog.dir.renameTo(renamedDir)
      if (renameSuccessful) {
        checkpointLogStartOffsetsInDir(removedLog.dir.getParentFile)
        removedLog.dir = renamedDir
        // change the file pointers for log and index file
        for (logSegment <- removedLog.logSegments) {
          logSegment.log.setFile(new File(renamedDir, logSegment.log.file.getName))
          logSegment.index.file = new File(renamedDir, logSegment.index.file.getName)
        }

        logsToBeDeleted.add(removedLog)
        removedLog.removeLogMetrics()
        info(s"Log for partition ${removedLog.topicPartition} is renamed to ${removedLog.dir.getAbsolutePath} and is scheduled for deletion")
      } else {
        throw new KafkaStorageException("Failed to rename log directory from " + removedLog.dir.getAbsolutePath + " to " + renamedDir.getAbsolutePath)
      }
    }
  }

  /**
   * Choose the next directory in which to create a log. Currently this is done
   * by calculating the number of partitions in each directory and then choosing the
   * data directory with the fewest partitions.
   */
  private def nextLogDir(): File = {
    //只有一个log目录
    if(logDirs.size == 1) {
      logDirs(0)
    } else {
      // count the number of logs in each parent directory (including 0 for empty directories
      //计算每个log目录中log数量
      val logCounts = allLogs.groupBy(_.dir.getParent).mapValues(_.size)
      val zeros = logDirs.map(dir => (dir.getPath, 0)).toMap
      val dirCounts = (zeros ++ logCounts).toBuffer
    
      // choose the directory with the least logs in it
      //选择log最少得log目录
      val leastLoaded = dirCounts.sortBy(_._2).head
      new File(leastLoaded._1)
    }
  }

  /**
   * Delete any eligible logs. Return the number of segments deleted.
   * Only consider logs that are not compacted.
   */
  //按照两个条件进行LogSegment清理
  //1.LogSegment的存活时间
  //2.整个log的大小
  //不仅会将过期的删除，还会根据log的大小删除过期的LogSegment控制整个log大小
  def cleanupLogs() {
    debug("Beginning log cleanup...")
    var total = 0
    val startMs = time.milliseconds
    //如果Log的 clean.policy 的配置不为delete.则不会进行删除
    for(log <- allLogs; if !log.config.compact) {
      debug("Garbage collecting '" + log.name + "'")
      total += log.deleteOldSegments()
    }
    debug("Log cleanup completed. " + total + " files deleted in " +
                  (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Get all the partition logs
   */
  def allLogs(): Iterable[Log] = logs.values

  /**
   * Get a map of TopicPartition => Log
   */
  def logsByTopicPartition: Map[TopicPartition, Log] = logs.toMap

  /**
   * Map of log dir to logs by topic and partitions in that dir
   */
  private def logsByDir = {
    this.logsByTopicPartition.groupBy {
      case (_, log) => log.dir.getParent
    }
  }

  /**
   * Flush any log which has exceeded its flush interval and has unwritten messages.
   */
  //根据配置的时长定时进行flush操作。保证数据的持久性
  private def flushDirtyLogs() = {
    debug("Checking for dirty logs to flush...")
    //遍历logs集合
    for ((topicPartition, log) <- logs) {
      try {
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        debug("Checking if flush is needed on " + topicPartition.topic + " flush interval  " + log.config.flushMs +
              " last flushed " + log.lastFlushTime + " time since last flush: " + timeSinceLastFlush)
        //检测是否到时间执行flush操作
        if(timeSinceLastFlush >= log.config.flushMs)
          log.flush//刷新
      } catch {
        case e: Throwable =>
          error("Error flushing topic " + topicPartition.topic, e)
      }
    }
  }
}

object LogManager {
  def apply(config: KafkaConfig,
            zkUtils: ZkUtils,
            brokerState: BrokerState,
            kafkaScheduler: KafkaScheduler,
            time: Time,
            brokerTopicStats: BrokerTopicStats): LogManager = {
    val defaultProps = KafkaServer.copyKafkaConfigToLog(config)
    val defaultLogConfig = LogConfig(defaultProps)

    val topicConfigs = AdminUtils.fetchAllTopicConfigs(zkUtils).map { case (topic, configs) =>
      topic -> LogConfig.fromProps(defaultProps, configs)
    }

    // read the log configurations from zookeeper
    val cleanerConfig = CleanerConfig(numThreads = config.logCleanerThreads,
      dedupeBufferSize = config.logCleanerDedupeBufferSize,
      dedupeBufferLoadFactor = config.logCleanerDedupeBufferLoadFactor,
      ioBufferSize = config.logCleanerIoBufferSize,
      maxMessageSize = config.messageMaxBytes,
      maxIoBytesPerSecond = config.logCleanerIoMaxBytesPerSecond,
      backOffMs = config.logCleanerBackoffMs,
      enableCleaner = config.logCleanerEnable)

    new LogManager(logDirs = config.logDirs.map(new File(_)).toArray,
      topicConfigs = topicConfigs,
      defaultConfig = defaultLogConfig,
      cleanerConfig = cleanerConfig,
      ioThreads = config.numRecoveryThreadsPerDataDir,
      flushCheckMs = config.logFlushSchedulerIntervalMs,
      flushRecoveryOffsetCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
      flushStartOffsetCheckpointMs = config.logFlushStartOffsetCheckpointIntervalMs,
      retentionCheckMs = config.logCleanupIntervalMs,
      maxPidExpirationMs = config.transactionIdExpirationMs,
      scheduler = kafkaScheduler,
      brokerState = brokerState,
      time = time,
      brokerTopicStats = brokerTopicStats)
  }
}
