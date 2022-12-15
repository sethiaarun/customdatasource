package com.ms.hdi.spark.datasource.model

import net.andreinc.mockneat.MockNeat
import net.andreinc.mockneat.abstraction.MockUnit

/**
 * base trait to generate data for given model
 */
trait DataGenObj {

  /**
   * Mockneat List of Parameters for the case class or Bean
   * The MockUnit is going to use the same to create the mockUnit
   * @return
   */
  def mockNeatParameters: (MockNeat, Int) => List[Object]

}
