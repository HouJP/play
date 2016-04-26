package com.houjp.ijcai16.datastructure

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class MerchantInfo(val merchant_id: String,
                   val budge: Int,
                   val location_id_list: Array[String]) {

}

object MerchantInfo {

  def parse(s: String): MerchantInfo = {

    val Array(merchant_id, budge_s, location_id_list_s) = s.split(",")
    new MerchantInfo(merchant_id, budge_s.toInt, location_id_list_s.split(":"))
  }

  def load(sc: SparkContext, pt: String): RDD[MerchantInfo] = {

    sc.textFile(pt).map(MerchantInfo.parse)
  }
}