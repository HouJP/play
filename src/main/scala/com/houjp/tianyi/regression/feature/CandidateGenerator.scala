package com.houjp.tianyi.regression.feature

import com.houjp.tianyi.datastructure.RawPoint
import org.apache.spark.rdd.RDD

object CandidateGenerator {

  def run(vvd: RDD[RawPoint], t_wid: Int, w_len: Int): RDD[(String, Int)] = {
    vvd.filter {
      e =>
        (e.wid < t_wid) && (e.wid >= t_wid - w_len)
    }.map(e => (e.uid, e.vid)).distinct()
  }
}