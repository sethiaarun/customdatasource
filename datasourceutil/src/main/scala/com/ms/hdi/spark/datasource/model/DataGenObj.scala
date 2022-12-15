package com.ms.hdi.spark.datasource.model

import net.andreinc.mockneat.MockNeat

/**
 * base trait for business model companion object
 */
trait DataGenObj {

  /**
   * List of Parameters for the case class or Bean
   * The MockUnit is going to use the same to create the mockUnit
   * @return
   */
  def mockNeatParameters: (MockNeat, Int) => List[Object]

}
