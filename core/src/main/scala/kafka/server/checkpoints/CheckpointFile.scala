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
package kafka.server.checkpoints

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.{FileAlreadyExistsException, FileSystems, Files, Paths}
import kafka.utils.{Exit, Logging}
import org.apache.kafka.common.utils.Utils
import scala.collection.{Seq, mutable}

trait CheckpointFileFormatter[T]{
  def toLine(entry: T): String

  def fromLine(line: String): Option[T]
}

class CheckpointFile[T](val file: File, version: Int, formatter: CheckpointFileFormatter[T]) extends Logging {
  private val path = file.toPath.toAbsolutePath
  private val tempPath = Paths.get(path.toString + ".tmp")
  private val lock = new Object()
  
  try Files.createFile(file.toPath) // create the file if it doesn't exist
  catch { case _: FileAlreadyExistsException => }

  //先将log目录下的所有log的recoveryPoint写入tmp文件中
  //用tmp文件替换原来的RecoveryPointCheckpint文件中
  def write(entries: Seq[T]) {
    lock synchronized {//加锁
      // write to temp file and then swap with the existing file
      val fileOutputStream = new FileOutputStream(tempPath.toFile)
      val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))
      try {
        //写入版本号
        writer.write(version.toString)
        writer.newLine()
        //写入记录条数
        writer.write(entries.size.toString)
        writer.newLine()
        //循环写入topic名称，分区编号以及对应的Log的revoveryPoint
        entries.foreach { entry =>
          writer.write(formatter.toLine(entry))
          writer.newLine()
        }

        writer.flush()
        //将数据刷新到磁盘
        fileOutputStream.getFD().sync()
      } catch {
        case e: FileNotFoundException =>
          if (FileSystems.getDefault.isReadOnly) {
            fatal(s"Halting writes to checkpoint file (${file.getAbsolutePath}) because the underlying file system is inaccessible: ", e)
            Exit.halt(1)
          }
          throw e
      } finally {
        writer.close()
      }
      //使用tmp临时文件替换原来的RecoveryPointCheckpoint文件
      Utils.atomicMoveWithFallback(tempPath, path)
    }
  }

  def read(): Seq[T] = {
    def malformedLineException(line: String) =
      new IOException(s"Malformed line in checkpoint file (${file.getAbsolutePath}): $line'")

    lock synchronized {
      val reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))
      var line: String = null
      try {
        line = reader.readLine()
        if (line == null)
          return Seq.empty
        line.toInt match {
          case fileVersion if fileVersion == version =>
            line = reader.readLine()
            if (line == null)
              return Seq.empty
            val expectedSize = line.toInt
            val entries = mutable.Buffer[T]()
            line = reader.readLine()
            while (line != null) {
              val entry = formatter.fromLine(line)
              entry match {
                case Some(e) =>
                  entries += e
                  line = reader.readLine()
                case _ => throw malformedLineException(line)
              }
            }
            if (entries.size != expectedSize)
              throw new IOException(s"Expected $expectedSize entries in checkpoint file (${file.getAbsolutePath}), but found only ${entries.size}")
            entries
          case _ =>
            throw new IOException(s"Unrecognized version of the checkpoint file (${file.getAbsolutePath}): " + version)
        }
      } catch {
        case _: NumberFormatException => throw malformedLineException(line)
      } finally {
        reader.close()
      }
    }
  }
}
