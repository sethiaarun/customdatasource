package com.ms.hdi.spark.datasource.model

import net.andreinc.mockneat.MockNeat

/**
 * base trait to generate data for given model
 */
trait DataGenObj {

  /**
   * generate data using mockneat
   *
   * @param mockNeat
   * @return
   */
  def generateData(mockNeat: MockNeat): BaseDataGen
}
