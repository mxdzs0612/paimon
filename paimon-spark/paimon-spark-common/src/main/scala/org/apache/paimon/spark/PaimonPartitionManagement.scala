/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark

import org.apache.paimon.CoreOptions
import org.apache.paimon.operation.FileStoreCommit
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.BatchWriteBuilder
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.{InternalRowPartitionComputer, TypeUtils}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.SupportsAtomicPartitionManagement
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap, Objects, UUID}

import scala.collection.JavaConverters._

trait PaimonPartitionManagement extends SupportsAtomicPartitionManagement {
  self: SparkTable =>

  lazy val partitionRowType: RowType = TypeUtils.project(table.rowType, table.partitionKeys)

  override lazy val partitionSchema: StructType = SparkTypeUtils.fromPaimonRowType(partitionRowType)

  private def toPaimonPartitions(rows: Array[InternalRow]): Array[java.util.Map[String, String]] = {
    table match {
      case fileStoreTable: FileStoreTable =>
        val rowConverter = CatalystTypeConverters
          .createToScalaConverter(CharVarcharUtils.replaceCharVarcharWithString(partitionSchema))
        val rowDataPartitionComputer = new InternalRowPartitionComputer(
          fileStoreTable.coreOptions().partitionDefaultName(),
          partitionRowType,
          table.partitionKeys().asScala.toArray,
          CoreOptions.fromMap(table.options()).legacyPartitionName)

        rows.map {
          r =>
            rowDataPartitionComputer
              .generatePartValues(new SparkRow(partitionRowType, rowConverter(r).asInstanceOf[Row]))
        }
      case _ =>
        throw new UnsupportedOperationException("Only FileStoreTable supports partitions.")
    }
  }

  override def dropPartitions(rows: Array[InternalRow]): Boolean = {
    table match {
      case fileStoreTable: FileStoreTable =>
        val partitions = toPaimonPartitions(rows).toSeq.asJava
        val partitionHandler = fileStoreTable.catalogEnvironment().partitionHandler()
        if (partitionHandler != null) {
          try {
            partitionHandler.dropPartitions(partitions)
          } finally {
            partitionHandler.close()
          }
        } else {
          val commit = fileStoreTable.newBatchWriteBuilder().newCommit()
          try {
            commit.truncatePartitions(partitions)
          } finally {
            commit.close()
          }
        }
        true

      case _ =>
        throw new UnsupportedOperationException("Only FileStoreTable supports drop partitions.")
    }
  }

  override def replacePartitionMetadata(
      ident: InternalRow,
      properties: JMap[String, String]): Unit = {
    throw new UnsupportedOperationException("Replace partition is not supported")
  }

  override def loadPartitionMetadata(ident: InternalRow): JMap[String, String] = {
    Map.empty[String, String].asJava
  }

  override def listPartitionIdentifiers(
      partitionCols: Array[String],
      internalRow: InternalRow): Array[InternalRow] = {
    assert(
      partitionCols.length == internalRow.numFields,
      s"Number of partition names (${partitionCols.length}) must be equal to " +
        s"the number of partition values (${internalRow.numFields})."
    )
    assert(
      partitionCols.forall(fieldName => partitionSchema.fieldNames.contains(fieldName)),
      s"Some partition names ${partitionCols.mkString("[", ", ", "]")} don't belong to " +
        s"the partition schema '${partitionSchema.sql}'."
    )
    table.newReadBuilder.newScan.listPartitions.asScala
      .map(binaryRow => DataConverter.fromPaimon(binaryRow, partitionRowType))
      .filter(
        sparkInternalRow => {
          partitionCols.zipWithIndex
            .map {
              case (partitionName, index) =>
                val internalRowIndex = partitionSchema.fieldIndex(partitionName)
                val structField = partitionSchema.fields(internalRowIndex)
                Objects.equals(
                  sparkInternalRow.get(internalRowIndex, structField.dataType),
                  internalRow.get(index, structField.dataType))
            }
            .forall(identity)
        })
      .toArray
  }

  override def createPartitions(
      rows: Array[InternalRow],
      maps: Array[JMap[String, String]]): Unit = {
    table match {
      case fileStoreTable: FileStoreTable =>
        val partitions = toPaimonPartitions(rows)
        val partitionHandler = fileStoreTable.catalogEnvironment().partitionHandler()
        if (partitionHandler != null) {
          try {
            if (fileStoreTable.coreOptions().partitionedTableInMetastore()) {
              partitionHandler.createPartitions(partitions.toSeq.asJava)
            }
          } finally {
            partitionHandler.close()
          }
        }
      case _ =>
        throw new UnsupportedOperationException("Only FileStoreTable supports create partitions.")
    }
  }
}
