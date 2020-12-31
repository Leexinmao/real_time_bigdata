package com.realtime.data.utils

import scala.collection.mutable.ListBuffer

object RandomOptions {


  def apply[T](opts: (T, Int)*) = {
    val randomOptions = new RandomOptions[T]()
    randomOptions.totalWeight = (0 /: opts) (_ + _._2) // 计算出来总的比重
    opts.foreach {
      case (value, weight) => randomOptions.options ++= (1 to weight).map(_ => value)
    }
    randomOptions
  }
}

class RandomOptions[T] {
  var totalWeight: Int = _
  var options = ListBuffer[T]()

  /**
   * 获取随机的 Option 的值
   *
   * @return
   */
  def getRandomOption() = {
    options(RandomNumUtils.randomInt(0, totalWeight - 1))
  }


}
