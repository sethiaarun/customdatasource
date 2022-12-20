package com.ms.hdi.spark.datasource.mock.batch

import com.ms.hdi.spark.datasource.mock.options.BatchMockOptions
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}

/**
 * Mock data batch processing
 * @param userSchema
 * @param properties
 */
class MockBatch(val userSchema: StructType, val properties: JMap[String, String]) extends Batch {

  /**
   * create number of partitions, each partition will have unique seed value
   *
   * @return
   */
  override def planInputPartitions(): Array[InputPartition] = {
    val numOfPartition = BatchMockOptions.getNumOfPartitions(properties)
    val numOfTotalRecords = BatchMockOptions.getTotalNumberOfRecords(properties)
    val eachPartitionNumOfRecords = numOfTotalRecords / numOfPartition
    (1 to numOfTotalRecords by eachPartitionNumOfRecords).map { i =>
      new MockPartition(i, eachPartitionNumOfRecords)
    }.toArray[InputPartition]
  }


  override def createReaderFactory(): PartitionReaderFactory =  new MockPartitionReaderFactory(userSchema, properties)
}
