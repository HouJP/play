package com.houjp.ijcai16.datastructure

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Ans(val user_id: String,
          val location_id: String,
          val merchant_id_list: Array[String]) {

  override def toString: String = {
    s"$user_id,$location_id,${merchant_id_list.mkString(":")}"
  }
}

object Ans {

  def parse(s: String): Ans = {
    val Array(user_id, location_id, merchant_id_list) = s.split(",")
    new Ans(user_id, location_id, merchant_id_list.split(":"))
  }

  def load(sc: SparkContext, pt: String): RDD[Ans] = {
    sc.textFile(pt).map(Ans.parse)
  }

  def tupleToAns(data: RDD[(String, String, String)]): RDD[Ans] = {
    data.map {
      case (user_id: String, merchant_id: String, location_id: String) =>
        ((user_id, location_id), Array(merchant_id))
    }.reduceByKey(_++_).map {
      case ((user_id: String, location_id: String), merchant_id_list: Array[String]) =>
        new Ans(user_id, location_id, merchant_id_list)
    }
  }
}