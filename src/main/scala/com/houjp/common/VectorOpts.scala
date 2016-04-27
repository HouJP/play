package com.houjp.common

object VectorOpts {

  def calVectorLength(vec: Array[Double]): Double = {
    math.sqrt(vec.map(math.pow(_, 2.0)).sum)
  }

  def calCosSimilarity(v1: Array[Double], v2: Array[Double]): Double = {
    assert(v1.length == v2.length)

    val l1 = calVectorLength(v1)
    val l2 = calVectorLength(v2)

    if ((math.abs(l1) < 1e-8) || (math.abs(l2) < 1e-8)) {
      0.0
    } else {
      v1.zip(v2).map(ele => ele._1 * ele._2).sum / l1 / l2
    }
  }

  def str2Vector(str: String): Array[Double] = {
    str.split(",").map(a => a.toDouble)
  }
}