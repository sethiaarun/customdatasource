# Custom Data Source
This project provides library to create mock data using [MockNeat](https://www.mockneat.com/) and Spark Data Source V2 API.

It is useful in case you would like to test your spark application with the custom business model and would like to generate random data generation.

## Batch DataSource

The Batch data source will create data in batch format. Developers need to provide business model schema as a Scala case class.

The user can generate data sources based on their business scenario. First, the user needs to provide a business model in the form of a case class and a companion object. The companion object will have a definition of how to generate values for the given business domain using MockNeat.

The data source schema will be derived at run time from the case class; that should extend from [`com.ms.hdi.spark.datasource.model.BaseDataGen`](datasourceutil/src/main/scala/com/ms/hdi/spark/datasource/model/BaseDataGen.scala), and the companion object should extend from [`com.ms.hdi.spark.datasource.model.DataGenObj`](datasourceutil/src/main/scala/com/ms/hdi/spark/datasource/model/DataGenObj.scala).

For example, the customer model is defined using [`com.ms.hdi.spark.datasource.batch.model.Customer`](batchdatasource/src/main/scala/com/ms/hdi/spark/datasource/batch/model/Customer.scala), and the data generation object is defined by `com.ms.hdi.spark.datasource.batch.model.CustomerObj`.

The data source has the following configuration options:

- `BatchMockOptions.SCHEMA_CLASS_NAME` -> case class name for Schema/Model (with full package name)
- `BatchMockOptions.DATA_GEN_OBJECT_NAME` -> data generation as per Schema/Model (with full package name)
- `BatchMockOptions.NUM_OF_RECORDS` -> number of records to be generated
- `BatchMockOptions.NUM_OF_PARTITIONS` -> number of partitions

Each partition will have `BatchMockOptions.NUM_OF_RECORDS/BatchMockOptions.NUM_OF_PARTITIONS` records.

The code for generating customer data using Spark:

```scala
val sparkConf = new SparkConf()
sparkConf.set("spark.master", "local")
val spark = SparkSession
.builder()
.config(sparkConf)
.appName("DataGenExample").getOrCreate()

val data: DataFrame = spark.read.
  format("com.ms.hdi.spark.datasource.batch.mock").
  option(BatchMockOptions.NUM_OF_RECORDS, "<<total number of records>>").
  option(BatchMockOptions.SCHEMA_CLASS_NAME, "<<your case class full qualified name>>").
  option(BatchMockOptions.DATA_GEN_OBJECT_NAME, "<<companion object>>").
  option(BatchMockOptions.NUM_OF_PARTITIONS, "<<total number of partitions>>").
  load()

data.show(false)
```



## Stream Data Source

--- Work in progress