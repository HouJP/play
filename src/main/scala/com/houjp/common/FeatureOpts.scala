package com.houjp.common

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object FeatureOpts {

  def save(fs: RDD[(String, Array[Double])], fp: String): Unit = {

    fs.map {
      p =>
        s"${p._1}\t${p._2.mkString(",")}"
    }.saveAsTextFile(fp)
  }

  def load(sc: SparkContext, fp: String): RDD[(String, String)] = {
    sc.textFile(fp).map {
      line =>
        val Array(key, fs) = line.split("\t")
        (key, fs)
    }
  }

  def merge(fs1: RDD[(String, String)], fs2: RDD[(String, String)]): RDD[(String, String)] = {
    fs1.join(fs2).map {
      case (key: String, (fs_1: String, fs_2: String)) =>
        (key, s"$fs_1,$fs_2")
    }
  }
}