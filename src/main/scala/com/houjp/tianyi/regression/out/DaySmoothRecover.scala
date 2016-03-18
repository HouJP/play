package com.houjp.tianyi.regression.out

import com.houjp.tianyi
import com.houjp.tianyi.datastructure.AnsPoint
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object DaySmoothRecover {

  /** command line parameters */
  case class Params(ans_pre_fp: String = tianyi.project_pt + "/data/raw/video-visit-data.txt.small",
                    ans_aft_fp: String = tianyi.project_pt + "",
                    smooth_rec_ans_fp: String = tianyi.project_pt + "/data/fs/user-active-count.txt")

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)
    val default_params = Params()

    val parser = new OptionParser[Params]("") {
      head("", "1.0")
      opt[String]("ans_pre_fp")
        .text("")
        .action { (x, c) => c.copy(ans_pre_fp = x) }
      opt[String]("ans_aft_fp")
        .text("")
        .action { (x, c) => c.copy(ans_aft_fp = x) }
      opt[String]("smooth_rec_ans_fp")
        .text("")
        .action { (x, c) => c.copy(smooth_rec_ans_fp = x) }
      help("help").text("prints this usage text")
    }

    println("[INFO] In main function ...")

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params): Unit = {
    println("[INFO] in run function ...")
    val conf = new SparkConf()
      .setAppName(s"tianyi-final day-smooth")
      .set("spark.hadoop.validateOutputSpecs", "false")
    if (tianyi.is_local) {
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)

    val rec: RDD[(String, Array[Double])] = AnsPoint.read(sc, p.ans_pre_fp).join(AnsPoint.read(sc, p.ans_aft_fp)).map {
      case (uid: String, (v_pre: Array[Double], v_aft: Array[Double])) =>
        val v_10_pre = Array.fill[Double](10)(0.0)
        Range(0, 70).foreach {
          id =>
            v_10_pre(id % 10) += v_pre(id)
        }
        val v_10_aft = Array.fill[Double](10)(0.0)
        Range(0, 70).foreach {
          id =>
            v_10_aft(id % 10) += v_aft(id)
        }

        Range(0, 70).foreach {
          id =>
            v_aft(id) = v_10_aft(id % 10) * v_pre(id) / v_10_pre(id % 10)
        }
        (uid, v_aft)
    }

    AnsPoint.save(rec, p.smooth_rec_ans_fp, 1)
  }
}