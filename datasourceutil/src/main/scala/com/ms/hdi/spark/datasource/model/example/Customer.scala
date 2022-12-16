package com.ms.hdi.spark.datasource.model.example

import com.ms.hdi.spark.datasource.model.{BaseDataGen, DataGenObj}
import net.andreinc.mockneat.MockNeat

/**
 * Example Customer Model
 *
 * @param customerId
 * @param customerName
 * @param firstName
 * @param lastName
 * @param userName
 */
case class Customer(customerId: Int, customerName: String, firstName: String,
                    lastName: String, userName: String, email: String) extends BaseDataGen

/**
 * customer object, we need this companion object to generate data using mockneat
 * This object is require to define how to fill mock values for model properties
 */
object CustomerObj extends DataGenObj {
  val mockNeatParameters: (MockNeat, Int) => List[Object] = (mockNeat: MockNeat, index:Int) => List(
    mockNeat.intSeq.start(index).increment(1), // customerId
    mockNeat.names().full(),
    mockNeat.names().first(),
    mockNeat.names().last(),
    mockNeat.users(),
    mockNeat.emails()
  )
}