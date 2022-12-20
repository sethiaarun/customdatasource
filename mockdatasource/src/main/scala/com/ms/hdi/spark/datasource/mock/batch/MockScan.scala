package com.ms.hdi.spark.datasource.mock.batch

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}

/**
 * Provides logical information, like what the actual read schema is and
 * physical representation of a data source scan for batch queries.
 * like how many partitions the scanned data has, and how to read records from the partitions.
 */
class MockScan(val userSchema: StructType, val properties: JMap[String, String]) extends Scan {

  override def toBatch: Batch = new MockBatch(userSchema,properties)

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

}
