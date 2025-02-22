// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.parquet

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOWriter
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOWriter.FlushResult
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.arrow.{ArrowUtils, ArrowWriter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{GlutenUtils, NativeIOUtils}

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable

class NativeParquetOutputWriter(val path: String, dataSchema: StructType, timeZoneId: String, context: TaskAttemptContext) extends OutputWriter {

  val NATIVE_IO_WRITE_MAX_ROW_GROUP_SIZE: Int = SQLConf.get.getConf(LakeSoulSQLConf.NATIVE_IO_WRITE_MAX_ROW_GROUP_SIZE)

  private var recordCount = 0

  var flushResult: mutable.Map[String, util.List[FlushResult]] = mutable.Map.empty

  val arrowSchema: Schema = ArrowUtils.toArrowSchema(dataSchema, timeZoneId)

  protected val nativeIOWriter: NativeIOWriter = new NativeIOWriter(arrowSchema)



  GlutenUtils.setArrowAllocator(nativeIOWriter)
  nativeIOWriter.setRowGroupRowNumber(NATIVE_IO_WRITE_MAX_ROW_GROUP_SIZE)
  if (path.endsWith(".parquet")) {
    nativeIOWriter.addFile(path)
  } else {
    nativeIOWriter.withPrefix(path)
  }

  NativeIOUtils.setNativeIOOptions(nativeIOWriter, NativeIOUtils.getNativeIOOptions(context, new Path(path)))

  nativeIOWriter.initializeWriter()

  val allocator: BufferAllocator = nativeIOWriter.getAllocator

  protected val root: VectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)

  private val recordWriter: ArrowWriter = ArrowWriter.create(root)

  override def write(row: InternalRow): Unit = {

    recordWriter.write(row)
    recordCount += 1

    if (recordCount >= NATIVE_IO_WRITE_MAX_ROW_GROUP_SIZE) {
      recordWriter.finish()
      nativeIOWriter.write(root)
      recordWriter.reset()
      recordCount = 0
    }
  }

  override def close(): Unit = {
    recordWriter.finish()

    nativeIOWriter.write(root)
    flushResult = nativeIOWriter.flush().asScala

    recordWriter.reset()
    root.close()
    nativeIOWriter.close()
  }
}
