package com.ms.hdi.spark.datasource.batch.mock

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Map => JMap}
import scala.reflect.runtime.{universe => ru}

/**
 * Spark searches for a class named DefaultSource in a given data source package.
 * So we create DefaultSource class in the package. It should extend *TableProvider** interface.
 * This user can provide various options/configurations to this source with keys provided at [[BatchMockOptions]]
 * Schema case class must extends from [[Base]] , the schema class must be provided by [[BatchMockOptions.SCHEMA_CLASS_NAME]]
 * For the defined schema case class, user must provide data generation object, that should extends from DataGenObj. The
 * object name must be provided by [[BatchMockOptions.DATA_GEN_OBJECT_NAME]]
 */
class DefaultSource extends TableProvider {

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    getTable(null, Array.empty[Transform], caseInsensitiveStringMap.asCaseSensitiveMap()).schema()
  }

  override def getTable(structType: StructType, transforms: Array[Transform], properties: JMap[String, String]): Table = {
    if (structType == null) {
      val schemaClassType: ru.Type = BatchMockOptions.getSchemaClassType(properties)
      val schema = ScalaReflection.schemaFor(schemaClassType).dataType.asInstanceOf[StructType]
      new MockTable(schema, properties)
    } else {
      new MockTable(structType, properties)
    }
  }
}