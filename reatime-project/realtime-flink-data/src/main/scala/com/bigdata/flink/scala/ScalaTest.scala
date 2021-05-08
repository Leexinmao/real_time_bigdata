package com.bigdata.flink.scala

import org.apache.flink.streaming.api.scala._

object ScalaTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val listStream: DataStream[List[Int]] = env.fromElements(List(1,2,3,4,5,6))

    val mapStream: DataStream[List[Int]] = listStream.map(r => {
      r.toList
    })
    mapStream.print()

    env.execute("ScalaTest")



  }

}
