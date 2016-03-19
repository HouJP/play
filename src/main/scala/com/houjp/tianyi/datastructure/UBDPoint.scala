package com.houjp.tianyi.datastructure

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class UBDPoint(uid: String,
                    wid: Int,
                    did: Int,
                    hid: Int,
                    mid: Int,
                    l1: Int,
                    vcnt: Int)

object UBDPoint {

  def filter(ubd: RDD[UBDPoint], t_wid: Int, w_len: Int): RDD[UBDPoint] = {
    ubd.filter {
      p =>
        (p.wid < t_wid) && (p.wid >= t_wid - w_len)
    }
  }

  def parse(line: String, max: Int, l_map: Map[String, Int]): UBDPoint = {

    val Array(uid, f2, f3, vcnt_str) = line.split("\t")
    val wid_str = f2.substring(1, 2)
    val did_str = f2.substring(3, 4)
    val hid_str = f2.substring(4, 6)
    val mid_str = f2.substring(6, 8)
    val vcnt = math.min(vcnt_str.toInt, max)
    val l1 = l_map(f3.split(",")(0))

    UBDPoint(uid,
      wid_str.toInt,
      did_str.toInt,
      hid_str.toInt,
      mid_str.toInt,
      l1,
      vcnt)
  }

  def read(sc: SparkContext, fp: String, label_fp: String, max: Int): RDD[UBDPoint] = {
    val l_map = sc.textFile(label_fp).map {
      line =>
        val Array(w, id_s) = line.split("\t")
        (w, id_s.toInt)
    }.collectAsMap().toMap
    sc.textFile(fp).map(s => UBDPoint.parse(s, max, l_map))
  }

}