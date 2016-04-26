package com.houjp.tianyi.classification.feature

import com.houjp.common.FeatureOpts
import com.houjp.tianyi
import com.houjp.tianyi.datastructure.{UBDPoint, RawPoint}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object L115AdjacentContinueVisit {
  /** command line parameters */
  case class Params(vvd_fp: String = tianyi.project_pt + "/data/raw/video-visit-data.txt.small",
                    ubd_fp: String = tianyi.project_pt + "/data/raw/user-behavior-data.small",
                    label_fp: String = tianyi.project_pt + "/data/stat/label_l1_index",
                    out_fp: String = tianyi.project_pt + "/data/fs/l1-15-adjacent-continue-visit_6_5.txt",
                    t_wid: Int = 6,
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
      opt[String]("ubd_fp")
        .text("")
        .action { (x, c) => c.copy(ubd_fp = x) }
      opt[String]("label_fp")
        .text("")
        .action { (x, c) => c.copy(label_fp = x) }
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
      .setAppName(s"tianyi-final l1-15-adjacent-continue-visit")
      .set("spark.hadoop.validateOutputSpecs", "false")
    if (tianyi.is_local) {
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)

    val base = Array(180, 300, 720, 1440)
    val f_len = 1
    val vvd = RawPoint.load(sc, p.vvd_fp, Int.MaxValue)
    val cdd: RDD[(String, Array[Double])] = CandidateGenerator.run(vvd, p.t_wid, p.w_len).map((_, Array.fill[Double](f_len)(0.0)))

    val ubd = UBDPoint.read(sc, p.ubd_fp, p.label_fp, Int.MaxValue)
    val fs = UBDPoint.filter(ubd, p.t_wid, p.w_len).filter(_.l1 == 15).map {
      p =>
        (p.uid, p.wid)
    }.distinct().map {
      case (uid: String, wid: Int) =>
        (uid, Array(wid))
    }.reduceByKey(_++_).map {
      case (uid: String, arr: Array[Int]) =>
        val flag = Array.fill[Boolean](5)(false)
        arr.foreach {
          wid =>
            val id = p.t_wid - wid - 1
            flag(id) = true
        }
        val fs = Array.fill[Double](f_len)(0.0)
        if (flag(0)) {
          fs(0) = 1.0
        }
        var break = false
        Range(1, 5).foreach {
          id =>
            if ((fs(0) > 1e-6) && flag(id) && !break) {
              fs(0) += 1.0
            } else {
              break = true
            }
        }
        (uid, fs)
    }

    val fs_all = cdd.leftOuterJoin(fs).map {
      e =>
        (e._1, e._2._2.getOrElse(e._2._1))
    }

    FeatureOpts.save(fs_all, p.out_fp)
  }
}