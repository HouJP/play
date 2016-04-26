package com.houjp.tianyi.datastructure

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class RawPoint(uid: String,
                    wid: Int,
                    did: Int,
                    hid: Int,
                    mid: Int,
                    vid: Int,
                    vcnt: Int)

object RawPoint {

  def filter(rp: RDD[RawPoint], t_wid: Int, w_len: Int): RDD[RawPoint] = {
    rp.filter {
      p =>
        (p.wid < t_wid) && (p.wid >= t_wid - w_len)
    }
  }

  /**
    * Parse string to RawPoint.
    *
    * @param line
    * @return
    */
  def parse(line: String, max: Int): RawPoint = {
    val Array(uid, f2, f3, vcnt_str) = line.split("\t")
    val wid_str = f2.substring(1, 2)
    val did_str = f2.substring(3, 4)
    val hid_str = f2.substring(4, 6)
    val mid_str = f2.substring(6, 8)
    val vid_str = f3.substring(1)
    val vcnt = math.min(vcnt_str.toInt, max)

    RawPoint(uid,
      wid_str.toInt,
      did_str.toInt,
      hid_str.toInt,
      mid_str.toInt,
      vid_str.toInt,
      vcnt)
  }

  /**
    * load RawPoint from disk.
    *
    * @param sc
    * @param fp
    * @return
    */
  def load(sc: SparkContext, fp: String, max: Int): RDD[RawPoint] = {
    sc.textFile(fp).map(s =>RawPoint.parse(s, max))
  }
}