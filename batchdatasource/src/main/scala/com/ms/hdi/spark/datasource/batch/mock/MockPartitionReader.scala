package com.ms.hdi.spark.datasource.batch.mock

import com.ms.hdi.spark.datasource.model.DataGenObj
import com.ms.hdi.spark.datasource.util.ReflectionUtil
import net.andreinc.mockneat.MockNeat
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}
import scala.reflect.ClassTag
/**
 *  It's responsible for outputting data for a RDD partition
 */
class MockPartitionReader(val userSchema: StructType, val properties: JMap[String, String]) extends PartitionReader[InternalRow]{

  /**
   * mock neat
   */
  private lazy val mockNeat = MockNeat.threadLocal()

  /**
   * data generation object for given schema class
   */
  private lazy val obj: DataGenObj =ReflectionUtil.getClassInstance(BatchMockOptions.getDataGenObjectName(properties))

  var index = 0
  val total: Long = BatchMockOptions.getTotalNumberOfRecords(properties)
  def next = index < total

  def get(): InternalRow = {
    val encoder =getEncoder()
    val row: InternalRow = encoder.createSerializer().apply(obj.generateData(mockNeat))
    index = index + 1
    row
  }

  def close() = Unit

  /**
   * get encoder based on given schema class name
   * @return
   */
  private def getEncoder(): ExpressionEncoder[Any] = {
    val schemaClassType= BatchMockOptions.getSchemaClassType(properties)
    val cls=ScalaReflection.mirror.runtimeClass(schemaClassType)
    val serializer = ScalaReflection.serializerForType(schemaClassType)
    val deserializer = ScalaReflection.deserializerForType(schemaClassType)
    new ExpressionEncoder[Any](
      serializer,
      deserializer,
      ClassTag(cls))
  }

}