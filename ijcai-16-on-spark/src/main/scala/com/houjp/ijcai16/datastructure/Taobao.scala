package com.houjp.ijcai16.datastructure

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Taobao(val user_id: String,
             val seller_id: String,
             val item_id: String,
             val category_id: String,
             val online_action_id: Int,
             val time_stamp: String,
             val year: Int,
             val month: Int,
             val day: Int) extends Serializable {

  override def toString: String = {
    s"$user_id,$seller_id,$item_id,$category_id,$online_action_id,$time_stamp"
  }
}

object Taobao {

  def load(sc: SparkContext, pt: String): RDD[Taobao] = {
    sc.textFile(pt).map {
      s =>
        val subs = s.split(",")
        val time_stamp = subs(5)
        val year = time_stamp.substring(0, 4).toInt
        val month = time_stamp.substring(4, 6).toInt
        val day = time_stamp.substring(6, 8).toInt
        new Taobao(subs(0), subs(1), subs(2), subs(3), subs(4).toInt, subs(5), year, month, day)
    }
  }

  def save(data: RDD[Taobao], pt: String) = {
    data.saveAsTextFile(pt)
  }
}