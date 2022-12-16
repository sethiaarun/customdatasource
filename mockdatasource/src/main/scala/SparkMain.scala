import com.ms.hdi.spark.datasource.mock.batch.BatchMockOptions
import com.ms.hdi.spark.datasource.mock.constants.MockDataUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkMain extends App {

  val sparkConf = new SparkConf()
  sparkConf.set("spark.master", "local")
  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .appName("DataGenExample").getOrCreate()

  val data: DataFrame = spark.read.
    format(MockDataUtils.BATCH_ALT_NAME).
    option(BatchMockOptions.NUM_OF_RECORDS, "10").
    option(BatchMockOptions.SCHEMA_CLASS_NAME, "com.ms.hdi.spark.datasource.model.example.Customer").
    option(BatchMockOptions.DATA_GEN_OBJECT_NAME, "com.ms.hdi.spark.datasource.model.example.CustomerObj").
    option(BatchMockOptions.NUM_OF_PARTITIONS, "2").
    load()

  println(data.count())
  data.show(100, false)
  println(data.rdd.getNumPartitions)

}
