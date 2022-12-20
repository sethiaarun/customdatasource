package com.ms.hdi.spark.datasource.mock.options

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap


/**
 * Stream data source options
 */
object StreamMockOptions extends MockOptions {

  import java.util.{Map => JMap}

  /**
   * batch interval in milli seconds, default value 100 ms
   */
  val BATCH_INTERVAL_MS = "batch.interval"

  /**
   * batch size, default value 1
   */
  val EACH_BATCH_SIZE = "batch.size"

  /**
   * get each batch size, default value=1
   *
   * @param properties
   * @return
   */
  def getBatchSize(properties: JMap[String, String]): Int = {
    properties.getOrDefault(EACH_BATCH_SIZE, "1").toInt
  }

  /**
   * batch interval in milli seconds, default 100 ms
   *
   * @param properties
   * @return
   */
  def getBatchIntervalMs(properties: JMap[String, String]): Int = {
    properties.getOrDefault(BATCH_INTERVAL_MS, "100").toInt
  }

  /**
   * validate all mandatory required options
   * @param params
   * @return
   */
  override def validateOptions(params: CaseInsensitiveMap[String]): Unit= {
    super.validateOptions(params)
  }
}
