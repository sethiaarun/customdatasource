package com.ms.hdi.spark.datasource.mock.batch

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}
/**
 * Scan Builder
 * You can mix with filter push down to keep the needed information to push the filters to readers.
 */
class MockScanBuilder(val userSchema: StructType, val properties: JMap[String, String]) extends ScanBuilder {

  override def build(): Scan = new MockScan(userSchema, properties)
}
