package com.ms.hdi.spark.datasource.mock.batch

import com.ms.hdi.spark.datasource.mock.options.BatchMockOptions
import com.ms.hdi.spark.datasource.model.BaseDataGen
import com.ms.hdi.spark.datasource.util.ReflectionUtil
import net.andreinc.mockneat.MockNeat
import net.andreinc.mockneat.abstraction.MockUnit
import net.andreinc.mockneat.types.enums.RandomType
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe

/**
 * It's responsible for outputting data for a RDD partition
 */
class MockPartitionReader(val userSchema: StructType,
                          val properties: JMap[String, String],
                          val mockPartition: MockPartition) extends PartitionReader[InternalRow] {

  // get schema class type for which we are trying to generate data
  private val schemaClassType: universe.Type = BatchMockOptions.getSchemaClassType(properties)
  // get schema class for which we are trying to generate data
  private val schemaClass: Class[_] = ScalaReflection.mirror.runtimeClass(BatchMockOptions.getSchemaClassType(properties))

  // start index
  private var index: Int = mockPartition.partSeedValue
  // end index for data generation
  private val endIndexMockNeat: Int = mockPartition.partSeedValue + mockPartition.partitionNumOfRecords

  private val encoder = getEncoder();
  private var nextRow: InternalRow = _
  /**
   * mock neat
   */
  private lazy val mockNeat = new MockNeat(RandomType.SECURE, mockPartition.partSeedValue)

  /**
   * for data generation we need mock neat data generation object for given schema class
   */
  private val mockUnit: MockUnit[BaseDataGen] =
    ReflectionUtil.getMockUnit(BatchMockOptions.getDataGenObjectName(properties),schemaClass, mockNeat,mockPartition.partSeedValue)

  // find how many we have to create
  override def next(): Boolean = {
    if(index < endIndexMockNeat) {
      val row: InternalRow = encoder.createSerializer().apply(mockUnit.get())
      if(row!=null) {
        nextRow = row
        index+=1
        true
      } else {
        false
      }
    } else  {
      false
    }
  }

  /**
   * get next record
   *
   * @return
   */
  def get(): InternalRow = {
    assert(nextRow != null)
    nextRow
  }

  def close() = Unit

  /**
   * get encoder based on given schema class name
   *
   * @return
   */
  private def getEncoder(): ExpressionEncoder[Any] = {
    val serializer: Expression = ScalaReflection.serializerForType(schemaClassType)
    val deserializer = ScalaReflection.deserializerForType(schemaClassType)
    new ExpressionEncoder[Any](
      serializer,
      deserializer,
      ClassTag(schemaClass))
  }

}