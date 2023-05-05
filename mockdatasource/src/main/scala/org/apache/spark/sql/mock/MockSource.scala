package org.apache.spark.sql.mock

import com.ms.hdi.example.model.BaseDataGen
import com.ms.hdi.spark.datasource.mock.options.StreamMockOptions
import com.ms.hdi.spark.datasource.mock.{OffSetLongOffSetConvert, OffSetTypeOfLongOffSet}
import net.andreinc.mockneat.abstraction.MockUnit
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

import java.util.{Map => JMap}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable

/**
 * Mock source for Streaming
 */
class MockSource(val sqlContext: SQLContext, val mockSchema: StructType, val properties: JMap[String, String]) extends Source {

  private var running:Boolean = true

  // this is to trigger the data generation
  private val dataGenThread =  dataGeneratorStartingThread()

  // batch buffer to keep intermediate generated data
  private var batches = collection.mutable.ListBuffer.empty[(BaseDataGen, Long)]

  // offset
  private var offset: LongOffset = LongOffset(-1)

  /**
   * get current schema
   * @return
   */
  override def schema: StructType = mockSchema

  /**
   * get current offset
   * @return
   */
  override def getOffset: Option[Offset] = this.synchronized {
    if (offset.offset == -1) None else Some(offset)
  }

  /**
   * get batch data from given start and end offset
   * @param start
   * @param end
   * @return
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = this.synchronized {
    val s = start.convert(LongOffset(-1)).offset + 1
    val e = end.typeConvert(LongOffset(-1)).offset + 1
    val data: mutable.Seq[BaseDataGen] = batches
      .par
      .filter { case (_, idx) => idx >= s && idx <= e }
      .map { case (v, _) => v }
      .seq
    val encoder = StreamMockOptions.getEncoder(properties)
    if(!data.isEmpty) {
      val rdd: RDD[InternalRow] = sqlContext
        .sparkContext
        .parallelize(data)
        .map(d => encoder.createSerializer().apply(d))
      sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
    } else {
      sqlContext.internalCreateDataFrame(
        sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"), schema, isStreaming = true)
    }
  }

  /**
   * stop stream data generation
   */
  override def stop(): Unit = {
    running=false
  }

  /**
   * commit the offset and clean the batches List
   * @param end
   */
  override def commit(end: Offset): Unit = this.synchronized {
    val committed = end.json().toLong
    val toKeep = batches.filter { case (_, idx) => idx > committed }
    println(s"cleared from batch: ${batches.size - toKeep.size}")
    batches = toKeep
  }


  /**
   * generate data using mockneat in given interval in given batch
   *  This function is going to get batch size and batch interval from options
   *  and It will generate data in given interval
   * @return
   */
  private def dataGeneratorStartingThread(): Thread = {
    val batchSize = StreamMockOptions.getBatchSize(properties)
    val batchInterval = StreamMockOptions.getBatchIntervalMs(properties)
    val mockUnit: MockUnit[BaseDataGen] = StreamMockOptions.getMockUnit(properties)
    val t = new Thread("increment") {
      setDaemon(true)
      override def run(): Unit = {
        while (running) {
          try {
            this.synchronized {
              // get batch size mock data
              val listMockData: List[BaseDataGen] = mockUnit.list(batchSize).get().toList
              // increment offset
              listMockData.map(md=>{
                offset = offset + 1
                batches.append((md,offset.offset))
              })
            }
          } catch {
            case e: Exception => println(e)
          }
          Thread.sleep(batchInterval)
        }
      }
    }
    t.start()
    t
  }
}
