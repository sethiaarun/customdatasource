package com.ms.hdi.spark.datasource.mock.batch

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.{Map => JMap}

/**
 * [[Table]] class representing a logical structured data set of a data source.
 * [[SupportsRead] adds newScanBuilder(CaseInsensitiveStringMap) that is
 * used to create a scan for batch, micro-batch, or continuous processing.
 */
class MockTable(val userSchema: StructType, val userProperties: JMap[String, String]) extends Table with SupportsRead {


  /**
   * Scan Builder
   * @param caseInsensitiveStringMap
   * @return
   */
  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = new MockScanBuilder(userSchema,userProperties)

  /**
   * name
   *
   * @return
   */
  override def name(): String = "MockCustomerTable"

  /**
   * customer schema
   *
   * @return
   */
  override def schema(): StructType = userSchema

  import scala.collection.JavaConverters._
  /**
   * we are specifying the source has batch reading capabilities.
   * @return
   */
  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ).asJava
  }
}