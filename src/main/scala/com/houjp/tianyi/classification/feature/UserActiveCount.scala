package com.houjp.tianyi.classification.feature

import com.houjp.tianyi
import com.houjp.tianyi.classification.FeatureOpts
import com.houjp.tianyi.datastructure.RawPoint
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object UserActiveCount {

  /** command line parameters */
  case class Params(vvd_fp: String = tianyi.project_pt + "/data/raw/video-visit-data.txt.small",
                    out_fp: String = tianyi.project_pt + "/data/fs/classification/user-active-count.txt",
                    t_wid: Int = 6,
                    w_len: Int = 5)

  def main(args: Array[String]) {
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
      opt[Int]("t_wid")
        .text("")
        .action { (x, c) => c.copy(t_wid = x) }
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

  def run(p: Params): Unit = {
    val conf = new SparkConf()
      .setAppName(s"tianyi-final user-active-count")
      .set("spark.hadoop.validateOutputSpecs", "false")
    if (tianyi.is_local) {
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)

    val f_len = 1
    val vvd = RawPoint.read(sc, p.vvd_fp, Int.MaxValue)
    val cdd = CandidateGenerator.run(vvd, p.t_wid, p.w_len).map((_, Array.fill[Double](f_len)(0.0)))
    val fs = RawPoint.filter(vvd, p.t_wid, p.w_len).map {
      p =>
        (p.uid, p.wid, p.did)
    }.distinct().map {
      p =>
        (p._1, 1)
    }.reduceByKey(_+_).map {
      p =>
        (p._1, Array(p._2.toDouble))
    }
    val fs_all = cdd.leftOuterJoin(fs).map {
      e =>
        (e._1, e._2._2.getOrElse(e._2._1))
    }

    FeatureOpts.save(fs, p.out_fp)
  }
}