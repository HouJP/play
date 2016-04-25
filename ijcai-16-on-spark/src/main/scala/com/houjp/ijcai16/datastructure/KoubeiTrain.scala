package com.houjp.ijcai16.datastructure

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class KoubeiTrain(val user_id: String,
                  val merchant_id: String,
                  val location_id: String,
                  val time_stamp: String,
                  val year: Int,
                  val month: Int,
                  val day: Int) extends Serializable {

  override def toString: String = {
    s"$user_id,$merchant_id,$location_id,$time_stamp"
  }
}

object KoubeiTrain {

  def load(sc: SparkContext, pt: String): RDD[KoubeiTrain] = {
    sc.textFile(pt).map {
      s =>
        val subs = s.split(",")
        val time_stamp = subs(3)
        val year = time_stamp.substring(0, 4).toInt
        val month = time_stamp.substring(4, 6).toInt
        val day = time_stamp.substring(6, 8).toInt
        new KoubeiTrain(subs(0), subs(1), subs(2), subs(3), year, month, day)
    }
  }

  def save(data: RDD[KoubeiTrain], pt: String) = {
    data.saveAsTextFile(pt)
  }
}