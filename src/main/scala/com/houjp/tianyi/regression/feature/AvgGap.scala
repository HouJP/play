package com.houjp.tianyi.regression.feature

import com.houjp.tianyi
import com.houjp.tianyi.datastructure.RawPoint
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object AvgGap {

  /** command line parameters */
  case class Params(vvd_fp: String = tianyi.project_pt + "/data/raw/video-visit-data.txt.small",
                    out_fp: String = tianyi.project_pt + "/data/fs/avggap_6_5",
                    w_tid: Int = 6,
                    w_len: Int = 5)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)
    val default_params = Params()

    val parser = new OptionParser[Params]("") {
      head("", "1.0")
      opt[String]("vvd_fp")
        .text("")
        .action { (x, c) => c.copy(vvd_fp = x) }
      opt[String]("out_fp")
        .text("")
        .action { (x, c) => c.copy(out_fp = x) }
      opt[Int]("w_tid")
        .text("")
        .action { (x, c) => c.copy(w_tid = x) }
      opt[Int]("w_len")
        .text("")
        .action { (x, c) => c.copy(w_len = x) }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params) = {
    val conf = new SparkConf()
      .setAppName(s"BDA AvgGap")
      .set("spark.hadoop.validateOutputSpecs", "false")
    if (tianyi.is_local) {
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)

    val vvd = RawPoint.load(sc, params.vvd_fp, Int.MaxValue)

    RawPoint.filter(vvd, params.w_tid, params.w_len).map(e => (e.uid, e.vid, e.wid, e.did)).distinct().map {
      case (uid: String, vid: Int, _, _) =>
        ((uid, vid), 1)
    }.reduceByKey(_+_).map {
      case ((uid: String, vid: Int), cnt: Int) =>
        s"$uid,$vid\t${7.0 * params.w_len / cnt}"
    }.saveAsTextFile(params.out_fp)
  }
}