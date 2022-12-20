package com.ms.hdi.spark.datasource.mock.options

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap


/**
 * Batch data source options
 */
object BatchMockOptions extends MockOptions {

  import java.util.{Map => JMap}

  /**
   * number of records to generate
   */
  val NUM_OF_RECORDS = "datagen.num.records"

  /**
   * number of records to generate
   */
  val NUM_OF_PARTITIONS = "datagen.num.partitions"


  /**
   * get total number of records
   *
   * @param properties
   * @return
   */
  def getTotalNumberOfRecords(properties: JMap[String, String]): Int = {
    properties.getOrDefault(NUM_OF_RECORDS, "0").toInt
  }

  /**
   * get number of partitions
   *
   * @param properties
   * @return
   */
  def getNumOfPartitions(properties: JMap[String, String]): Int = {
    properties.getOrDefault(NUM_OF_PARTITIONS, "1").toInt
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
