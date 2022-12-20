package com.ms.hdi.spark.datasource.mock.constants

import com.ms.hdi.spark.datasource.mock.options.BatchMockOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

object MockDataUtils {

  // [[BatchMockDataSource]] This allows users to give the data source alias as the format type over the
  // fully qualified class name for mock data source
  val BATCH_ALT_NAME = "batchMockDataSource"

  val STREAM_ALT_NAME = "streamMockDataSource"
}
