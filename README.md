# Custom Data Source
This project provides library to create mock data using [MockNeat](https://www.mockneat.com/) and Spark Data Source V2 API.

It is useful in case you would like to test your spark application with the custom business model and would like to generate random data generation.

## Batch DataSource

The Batch data source will create data in batch format. Developers need to provide business model schema as a Scala case class.

For data generation for a given schema, the user should provide an object extending from `com.ms.hdi.spark.datasource.model.DataGenObj`.

For example, the customer model is defined using `com.ms.hdi.spark.datasource.batch.model.Customer`, and the data generation object is defined by `com.ms.hdi.spark.datasource.batch.model.CustomerObj`.

The data source has the following configuration options:

- `BatchMockOptions.SCHEMA_CLASS_NAME` -> case class name for Schema/Model (with full package name)
- `BatchMockOptions.DATA_GEN_OBJECT_NAME` -> data generation as per Schema/Model (with full package name)
- `BatchMockOptions.NUM_OF_RECORDS` -> number of records to be generated

The code for generating customer data using Spark:

```scala
val sparkConf = new SparkConf()
sparkConf.set("spark.master", "local")
val spark = SparkSession
.builder()
.config(sparkConf)
.appName("DataGenExample").getOrCreate()

val data = spark.read.
format("com.ms.hdi.spark.datasource.batch.mock").
option(BatchMockOptions.NUM_OF_RECORDS, "100").
option(BatchMockOptions.SCHEMA_CLASS_NAME, "com.ms.hdi.spark.datasource.batch.model.Customer").
option(BatchMockOptions.DATA_GEN_OBJECT_NAME, "com.ms.hdi.spark.datasource.batch.model.CustomerObj").
load()
```

## Stream Data Source

--- Work in progress