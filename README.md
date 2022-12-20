# Custom Data Source
This project provides library to create mock data using [MockNeat](https://www.mockneat.com/) and Spark Data Source V2 API.

It is useful in case you would like to test your spark application with the custom business model (Transactional and/or Master data) and would like to generate random deterministic data generation.

## Batch DataSource

The Batch data source will create data in batch format. Developers need to provide business model schema as a Scala case class.

The user can generate data sources based on their business scenario. First, the user needs to provide a business model in the form of a case class and a companion object. The companion object will have a definition of how to generate values for the given business domain using MockNeat.

The data source schema will be derived at run time from the case class; that should extend from [`com.ms.hdi.spark.datasource.model.BaseDataGen`](datasourceutil/src/main/scala/com/ms/hdi/spark/datasource/model/BaseDataGen.scala), and the companion object should extend from [`com.ms.hdi.spark.datasource.model.DataGenObj`](datasourceutil/src/main/scala/com/ms/hdi/spark/datasource/model/DataGenObj.scala).

For example, the customer model is defined using [`com.ms.hdi.spark.datasource.model.example.Customer`](datasourceutil/src/main/scala/com/ms/hdi/spark/datasource/model/example/Customer.scala), and the data generation object is defined by `com.ms.hdi.spark.datasource.model.example.CustomerObj`.

The data source has the following configuration options:

- `BatchMockOptions.SCHEMA_CLASS_NAME` -> case class name for Schema/Model (with full package name)
- `BatchMockOptions.DATA_GEN_OBJECT_NAME` -> data generation as per Schema/Model (with full package name)
- `BatchMockOptions.NUM_OF_RECORDS` -> number of records to be generated
- `BatchMockOptions.NUM_OF_PARTITIONS` -> number of partitions

Each partition will have `BatchMockOptions.NUM_OF_RECORDS/BatchMockOptions.NUM_OF_PARTITIONS` records.

The code for generating batch customer data using Spark:

```scala
import com.ms.hdi.spark.datasource.mock.batch.BatchMockOptions
import com.ms.hdi.spark.datasource.mock.constants.MockDataUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
val sparkConf = new SparkConf()
sparkConf.set("spark.master", "local")
val spark = SparkSession
.builder()
.config(sparkConf)
.appName("DataGenExample").getOrCreate()

val data: DataFrame = spark.read.
  format(MockDataUtils.BATCH_ALT_NAME).
  option(BatchMockOptions.NUM_OF_RECORDS, "<<total number of records>>").
  option(BatchMockOptions.SCHEMA_CLASS_NAME, "<<your case class full qualified name>>").
  option(BatchMockOptions.DATA_GEN_OBJECT_NAME, "<<companion object>>").
  option(BatchMockOptions.NUM_OF_PARTITIONS, "<<total number of partitions>>").
  load()

data.show(false)
```
## Stream Data Source

The Stream data source will create data in stream. Developers need to provide business model schema as a Scala case class.

The user can generate data sources based on their business scenario. First, the user needs to provide a business model in the form of a case class and a companion object. The companion object will have a definition of how to generate values for the given business domain using MockNeat.

The data source schema will be derived at run time from the case class; that should extend from [`com.ms.hdi.spark.datasource.model.BaseDataGen`](datasourceutil/src/main/scala/com/ms/hdi/spark/datasource/model/BaseDataGen.scala), and the companion object should extend from [`com.ms.hdi.spark.datasource.model.DataGenObj`](datasourceutil/src/main/scala/com/ms/hdi/spark/datasource/model/DataGenObj.scala).

For example, the customer model is defined using [`com.ms.hdi.spark.datasource.model.example.Customer`](datasourceutil/src/main/scala/com/ms/hdi/spark/datasource/model/example/Customer.scala), and the data generation object is defined by `com.ms.hdi.spark.datasource.model.example.CustomerObj`.

The data source has the following configuration options:

- `StreamMockOptions.SCHEMA_CLASS_NAME` -> case class name for Schema/Model (with full package name)
- `StreamMockOptions.DATA_GEN_OBJECT_NAME` -> data generation as per Schema/Model (with full package name)
- `StreamMockOptions.EACH_BATCH_SIZE` -> batch size
- `StreamMockOptions.BATCH_INTERVAL_MS` -> batch interval

```scala

val sparkConf = new SparkConf()
sparkConf.set("spark.master", "local")

val spark = SparkSession
    .builder()
    .config(sparkConf)
    .appName("DataGenExample").getOrCreate()

val data: DataFrame = spark.readStream.
    format(MockDataUtils.STREAM_ALT_NAME).
    option(StreamMockOptions.EACH_BATCH_SIZE, "<<batch size>>").
    option(StreamMockOptions.SCHEMA_CLASS_NAME, "<<your case class full qualified name>>").
    option(StreamMockOptions.DATA_GEN_OBJECT_NAME, "<<companion object>>").
    option(StreamMockOptions.BATCH_INTERVAL_MS, "<<interval in milli seconds>>").load()

data.writeStream
    .format("console")
    .outputMode("append")
    .start()             // Start the computation
    .awaitTermination()
```

Developer can extend both Stream and Batch further based on your need.

### Complex Schema

For complex schemas, such as

```scala

case class Car(brandName:String)

case class Customer(customerId: Int, customerName: String, firstName: String,
                    lastName: String, userName: String, email: String, car:Car) extends BaseDataGen
```

We have two options:

1. Use MockNeat Transformation to transform parameter to object, for example:
```scala

  import com.ms.hdi.spark.datasource._
  val carFn = (s:String)=>Car(s)
  val mockNeatParameters: (MockNeat, Int) => List[Object] = (mockNeat: MockNeat, index:Int) => List(
    mockNeat.intSeq.start(index).increment(1), // customerId
    mockNeat.names().full(),
    mockNeat.names().first(),
    mockNeat.names().last(),
    mockNeat.users(),
    mockNeat.emails(),
    mockNeat.cars().brands().map(toJavaFunction(carFn))
  )
```
2. Create a custom MockUnit for the inner object (in this case ```Car``` )

## Installing

### Maven 

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

```
```xml
<dependency>
    <groupId>com.github.sethiaarun</groupId>
    <artifactId>customdatasource</artifactId>
    <version>TAG</version>
</dependency>
```

### SBT

```
resolvers += "jitpack" at "https://jitpack.io"
```

```
libraryDependencies += "com.github.sethiaarun" % "customdatasource" % "TAG"	
```

### Gradle

```json
allprojects {
    repositories {
        ...
        maven { url 'https://jitpack.io' }
    }
}
```

```json
dependencies {
  implementation 'com.github.sethiaarun:customdatasource:TAG'
}
```