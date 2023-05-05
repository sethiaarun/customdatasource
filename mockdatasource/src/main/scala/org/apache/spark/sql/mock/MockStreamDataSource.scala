package org.apache.spark.sql.mock

import com.ms.hdi.model.mockobj.DataGenObj
import com.ms.hdi.spark.datasource.mock.constants.MockDataUtils
import com.ms.hdi.spark.datasource.mock.options.StreamMockOptions
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}
import scala.jdk.CollectionConverters.mapAsJavaMapConverter
import scala.reflect.runtime.{universe => ru}

/**
 * Spark searches for a class named DefaultSource in a given data source package.
 * So we create DefaultSource class in the package. It should extend *TableProvider** interface.
 * This user can provide various options/configurations to this source with keys provided at [[StreamMockOptions]]
 * Schema case class must extends from [[com.ms.hdi.spark.datasource.model.BaseDataGen]], pass it by [[StreamMockOptions.SCHEMA_CLASS_NAME]]
 * For the defined schema case class, user must provide data generation object, that should extends from [[DataGenObj]]. The
 * object name must be provided by [[StreamMockOptions.DATA_GEN_OBJECT_NAME]]
 */
class MockStreamDataSource extends StreamSourceProvider
  with DataSourceRegister {

  override def shortName(): String = MockDataUtils.STREAM_ALT_NAME

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    (shortName(), getSchema(parameters.asJava))
  }

  /**
   * create datasource, It will discard any schema passed from option
   * This will get schema from given class name passed from option
   * @param sqlContext
   * @param metadataPath
   * @param schema
   * @param providerName
   * @param parameters
   * @return
   */
  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType],
                            providerName: String, parameters: Map[String, String]): Source = {
    val dataSchema = getSchema(parameters.asJava)
    new MockSource(sqlContext, dataSchema, parameters.asJava)
  }

  /**
   * get schema from given schema case class
   *
   * @param properties
   * @return
   */
  private def getSchema(properties: JMap[String, String]): StructType = {
    val schemaClassType: ru.Type = StreamMockOptions.getSchemaClassType(properties)
    ScalaReflection.schemaFor(schemaClassType).dataType.asInstanceOf[StructType]
  }
}