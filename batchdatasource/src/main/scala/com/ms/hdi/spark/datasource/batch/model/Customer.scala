package com.ms.hdi.spark.datasource.batch.model

import com.ms.hdi.spark.datasource.model.{BaseDataGen, DataGenObj}
import net.andreinc.mockneat.MockNeat

/**
 * Customer Model
 *
 * @param customerId
 * @param customerName
 * @param firstName
 * @param lastName
 * @param userName
 */
case class Customer(var customerId: Int, var customerName: String, var firstName: String,
                    var lastName: String, var userName: String) extends BaseDataGen

/**
 * customer object, we need this companion object to generate data using mockneat
 */
object CustomerObj extends DataGenObj {
  /**
   * generate data using mockneat
   * @param mockNeat
   * @return
   */
  def generateData(mockNeat: MockNeat): Customer = {
    Customer(mockNeat.ints().get(),
      mockNeat.names().full().get(),
      mockNeat.names().first().get(),
      mockNeat.names().last().get(),
      mockNeat.users().get())
  }
}