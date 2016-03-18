package com.houjp.tianyi.datastructure

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object AnsPoint {

  def read(sc: SparkContext, fp: String): RDD[(String, Array[Double])] = {
    sc.textFile(fp).map {
      line =>
        val Array(uid, v_s) = line.split("\t")
        (uid, v_s.split(",").map(_.toDouble))
    }
  }

  def save(aps: RDD[(String, Array[Double])], fp: String, scale: Int): Unit = {
    aps.map {
      case (uid: String, v: Array[Double]) =>
        s"$uid\t${v.map(_ * scale).map(_.toInt).mkString(",")}"
    }.saveAsTextFile(fp)
  }
}