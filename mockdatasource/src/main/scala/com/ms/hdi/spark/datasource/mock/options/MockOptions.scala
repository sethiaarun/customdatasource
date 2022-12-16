package com.ms.hdi.spark.datasource.mock.options

import com.ms.hdi.spark.datasource.util.ReflectionUtil

import java.util.{Map => JMap}
import scala.reflect.runtime.{universe => ru}

/**
 * Mock data source options
 */
trait MockOptions {

  /**
   * schema class name, It can be primitive type or case class
   */
  val SCHEMA_CLASS_NAME = "schema.class.name"

  /**
   * data generation object name
   */
  val DATA_GEN_OBJECT_NAME = "datagen.object.name"

  /**
   * number of records to generate
   */
  val NUM_OF_RECORDS = "datagen.num.records"

  /**
   * number of records to generate
   */
  val NUM_OF_PARTITIONS = "datagen.num.partitions"


  /**
   * get total number of records
   *
   * @param properties
   * @return
   */
  def getTotalNumberOfRecords(properties: JMap[String, String]): Int = {
    properties.getOrDefault(NUM_OF_RECORDS, "0").toInt
  }

  /**
   * get number of partitions
   *
   * @param properties
   * @return
   */
  def getNumOfPartitions(properties: JMap[String, String]): Int = {
    properties.getOrDefault(NUM_OF_PARTITIONS, "1").toInt
  }


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

}
