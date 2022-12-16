package com.ms.hdi.spark.datasource.mock.batch

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}
/**
 * A factory used to create PartitionReader instances.
 */
class MockPartitionReaderFactory(val userSchema: StructType, val properties: JMap[String, String]) extends PartitionReaderFactory {

  /**
   * Returns a row-based partition reader to read data from the given InputPartition.
   * Implementations probably need to cast the input partition to the concrete InputPartition class defined for the data source.
   * @param partition
   * @return
   */
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new MockPartitionReader(userSchema, properties,partition.asInstanceOf[MockPartition])
}
