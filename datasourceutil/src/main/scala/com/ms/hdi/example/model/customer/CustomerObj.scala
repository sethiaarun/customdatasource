package com.ms.hdi.example.model.customer

import com.ms.hdi.model.mockobj.DataGenObj
import net.andreinc.mockneat.MockNeat

/**
 * customer object, we need this companion object to generate data using mockneat
 * This object is require to define how to fill mock values for model properties
 */
object CustomerObj extends DataGenObj {
  val mockNeatParameters: (MockNeat, Int) => List[Object] = (mockNeat: MockNeat, index: Int) => List(
    mockNeat.intSeq.start(index).increment(1), // customerId
    mockNeat.names().full(),
    mockNeat.names().first(),
    mockNeat.names().last(),
    mockNeat.users(),
    mockNeat.emails()
  )
}
