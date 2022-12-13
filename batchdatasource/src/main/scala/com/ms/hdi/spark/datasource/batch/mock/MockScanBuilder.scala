package com.ms.hdi.spark.datasource.batch.mock

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import java.util.{Map => JMap}
/**
 * Scan Builder
 */
class MockScanBuilder(val userSchema: StructType, val properties: JMap[String, String]) extends ScanBuilder {

  override def build(): Scan = new MockScan(userSchema, properties)
}
