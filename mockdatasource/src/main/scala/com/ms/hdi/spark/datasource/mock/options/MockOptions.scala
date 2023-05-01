package com.ms.hdi.spark.datasource.mock.options

import com.ms.hdi.spark.datasource.model.BaseDataGen
import com.ms.hdi.spark.datasource.util.ReflectionUtil
import net.andreinc.mockneat.MockNeat
import net.andreinc.mockneat.abstraction.MockUnit
import net.andreinc.mockneat.types.enums.RandomType
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.{Encoder, Encoders}

import scala.reflect.api
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

trait MockOptions {

  import java.util.{Map => JMap}
  import scala.reflect.runtime.{universe => ru}

  /**
   * schema class name, It can be primitive type or case class
   */
  val SCHEMA_CLASS_NAME = "schema.class.name"

  /**
   * data generation object name
   */
  val DATA_GEN_OBJECT_NAME = "datagen.object.name"

  /**
   * get schema class name for given map
   *
   * @param properties
   * @return
   */
  def getSchemaClassName(properties: JMap[String, String]): String = properties.get(SCHEMA_CLASS_NAME)


  /**
   * get data generation object name
   *
   * @param properties
   * @return
   */
  def getDataGenObjectName(properties: JMap[String, String]): String = properties.get(DATA_GEN_OBJECT_NAME)

  /**
   * get schema class Type for given map
   *
   * @param properties
   * @return
   */
  def getSchemaClassType(properties: JMap[String, String]): ru.Type = {
    ReflectionUtil.getTypeFromStringClassName(getSchemaClassName(properties))
  }

  /**
   * validate all mandatory required options
   *
   * @param params
   * @return
   */
  def validateOptions(params: CaseInsensitiveMap[String]): Unit = {
    if (!params.get(BatchMockOptions.SCHEMA_CLASS_NAME).isDefined) {
      throw new IllegalArgumentException("schema class is missing")
    }
    if (!params.get(BatchMockOptions.DATA_GEN_OBJECT_NAME).isDefined) {
      throw new IllegalArgumentException("data generation object name is missing")
    }
  }


  /**
   * get type tag from given type
   * @param tpe
   * @tparam T
   * @return
   */
  private def backward[T](tpe: Type): TypeTag[T] = {
    val mirror = universe.runtimeMirror(getClass.getClassLoader)
    TypeTag(mirror, new api.TypeCreator {
      def apply[U <: api.Universe with Singleton](m: api.Mirror[U]) =
        if (m eq mirror) tpe.asInstanceOf[U#Type]
        else throw new IllegalArgumentException(s"Type tag defined in $ReflectionUtil.mirror cannot be migrated to other mirrors.")
    })
  }

  /**
   * get encoder based on given schema class name
   *
   * @return
   */
  def getEncoder(properties: JMap[String, String]): ExpressionEncoder[Any] = {
    // get schema class type for which we are trying to generate data
    val schemaClassType: universe.Type = BatchMockOptions.getSchemaClassType(properties)
    val typeTag: ru.TypeTag[Product] = backward(schemaClassType).asInstanceOf[TypeTag[Product]]
    val encoder: Encoder[Product] = Encoders.product(typeTag)
    encoder.asInstanceOf[ExpressionEncoder[Any]]
  }


  /**
   * get schema class from options
   *
   * @param properties
   * @return
   */
  def getSchemaClass(properties: JMap[String, String]): Class[_] = {
    val mirror = universe.runtimeMirror(getClass.getClassLoader)
    // get schema class type for which we are trying to generate data
    val schemaClassType: universe.Type = BatchMockOptions.getSchemaClassType(properties)
    // get schema class for which we are trying to generate data
    mirror.runtimeClass(schemaClassType)
  }




  /**
   * get mock unit based on schema class and object name
   *
   * @param properties
   * @param seedValue
   * @param index
   * @return
   */
  def getMockUnit(properties: JMap[String, String], seedValue: Long = 1, index: Int = 1): MockUnit[BaseDataGen] = {
    val schemaClass: Class[_] = getSchemaClass(properties)
    val objName: String = getDataGenObjectName(properties)
    val mockNeat = new MockNeat(RandomType.SECURE, seedValue)
    ReflectionUtil.getMockUnit(objName, schemaClass, mockNeat, 1)
  }
}
