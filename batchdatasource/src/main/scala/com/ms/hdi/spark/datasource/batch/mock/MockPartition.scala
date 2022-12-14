package com.ms.hdi.spark.datasource.batch.mock

import org.apache.spark.sql.connector.read.InputPartition

/**
 * number of partitions
 * @param partSeedValue partition seed value for mockneat
 * @param partitionNumOfRecords number of records in each partition
 */
class MockPartition(val partSeedValue:Int, val partitionNumOfRecords:Int) extends InputPartition
