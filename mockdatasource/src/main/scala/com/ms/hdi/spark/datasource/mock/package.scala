package com.ms.hdi.spark.datasource

import org.apache.spark.sql.execution.streaming.{LongOffset, Offset}

package object mock {

  /**
   * convert offset to LongOffset
   * @param offset
   */
  implicit class OffSetLongOffSetConvert(offset:Option[Offset]) {
    def convert(default:LongOffset): LongOffset ={
      offset match {
        case Some(v:LongOffset)=> v
        case _ => default
      }
    }
  }

  /**
   * convert offset to LongOffset
   * @param offset
   */
  implicit class OffSetTypeOfLongOffSet(offset:Offset) {
    def typeConvert(default:LongOffset): LongOffset ={
      offset match {
        case v:LongOffset=> v
        case _ => default
      }
    }
  }
}
