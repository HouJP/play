package com.houjp.tianyi.classification

import org.apache.spark.rdd.RDD

object FeatureOpts {

  def save(fs: RDD[(String, Array[Double])], fp: String): Unit = {

    fs.map {
      p =>
        s"${p._1}\t${p._2.mkString(",")}"
    }.saveAsTextFile(fp)
  }
}