package com.ms.hdi.spark.datasource.batch.mock

import com.ms.hdi.spark.datasource.model.{BaseDataGen, DataGenObj}
import com.ms.hdi.spark.datasource.util.ReflectionUtil
import net.andreinc.mockneat.MockNeat
import net.andreinc.mockneat.types.enums.RandomType
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}
import scala.reflect.ClassTag

/**
 * It's responsible for outputting data for a RDD partition
 */
class MockPartitionReader(val userSchema: StructType,
                          val properties: JMap[String, String],
                          val mockPartition: MockPartition) extends PartitionReader[InternalRow] {

  // start index
  private var index: Int = mockPartition.partSeedValue
  // total records for each partition
  private val total: Int = mockPartition.partitionNumOfRecords
  // start index for mockneat
  private val startIndexMockNeat: Int = mockPartition.partSeedValue
  // end index for mockneat
  private val endIndexMockNeat: Int = startIndexMockNeat + total

  /**
   * mock neat
   */
  private lazy val mockNeat = new MockNeat(RandomType.SECURE, mockPartition.partSeedValue)

  /**
   * data generation object for given schema class
   */
  private val dataGenObj: DataGenObj = ReflectionUtil.getClassInstance(BatchMockOptions.getDataGenObjectName(properties))

  // find how many we have to create
  def next() = index < endIndexMockNeat

  /**
   * get next record
   *
   * @return
   */
  def get(): InternalRow = {
    val encoder = getEncoder()
    val row: InternalRow = encoder.createSerializer().apply(
      dataGenObj.generateData(mockNeat, index)
    )
    index = index + 1
    row
  }

  def close() = Unit

  /**
   * get encoder based on given schema class name
   *
   * @return
   */
  private def getEncoder(): ExpressionEncoder[Any] = {
    val schemaClassType = BatchMockOptions.getSchemaClassType(properties)
    val cls = ScalaReflection.mirror.runtimeClass(schemaClassType)
    val serializer = ScalaReflection.serializerForType(schemaClassType)
    val deserializer = ScalaReflection.deserializerForType(schemaClassType)
    new ExpressionEncoder[Any](
      serializer,
      deserializer,
      ClassTag(cls))
  }

}