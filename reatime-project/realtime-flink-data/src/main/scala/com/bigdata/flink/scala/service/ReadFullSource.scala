package com.bigdata.flink.scala.service

import com.bigdata.flink.scala.source.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._

object ReadFullSource {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val source: DataStream[SensorReading] = env.addSource(new SensorSource)

    source.print()

    env.execute("ReadFullSource")

  }

}
