package com.ms.hdi.spark.datasource.mock.batch

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}

/**
 * Provides logical information, like what the actual read schema is and
 * physical representation of a data source scan for batch queries.
 * like how many partitions the scanned data has, and how to read records from the partitions.
 */
class MockScan(val userSchema: StructType, val properties: JMap[String, String]) extends Scan with Batch {

  override def toBatch: Batch = this

  /**
   * schema
   *
   * @return
   */
  override def readSchema(): StructType = userSchema

  /**
   * Description
   *
   * @return
   */
  override def description = "mock_customer_scan"

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

  /**
   *
   * @return
   */
  override def createReaderFactory(): PartitionReaderFactory = new MockPartitionReaderFactory(userSchema, properties)
}
