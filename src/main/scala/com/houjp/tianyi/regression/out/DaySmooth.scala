package com.houjp.tianyi.regression.out

import com.houjp.tianyi
import com.houjp.tianyi.datastructure.AnsPoint
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object DaySmooth {
  /** command line parameters */
  case class Params(ans_fp: String = tianyi.project_pt + "/data/raw/video-visit-data.txt.small",
                    smooth_ans_fp: String = tianyi.project_pt + "/data/fs/user-active-count.txt")


  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)
    val default_params = Params()

    val parser = new OptionParser[Params]("") {
      head("", "1.0")
      opt[String]("ans_fp")
        .text("")
        .action { (x, c) => c.copy(ans_fp = x) }
      opt[String]("smooth_ans_fp")
        .text("")
        .action { (x, c) => c.copy(smooth_ans_fp = x) }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }

    def run(p: Params): Unit = {
      val conf = new SparkConf()
        .setAppName(s"tianyi-final day-smooth")
        .set("spark.hadoop.validateOutputSpecs", "false")
      if (tianyi.is_local) {
        conf.setMaster("local[4]")
      }
      val sc = new SparkContext(conf)

      val aps = AnsPoint.read(sc, p.ans_fp).map {
        case (uid: String, v: Array[Double]) =>
          val v_10 = Array.fill[Double](10)(0.0)
          Range(0, 70).foreach {
            id =>
              v_10(id % 10) += v(id)
          }
          (uid, v_10 ++ v_10 ++ v_10 ++ v_10 ++ v_10 ++ v_10 ++ v_10)
      }

      AnsPoint.save(aps, p.smooth_ans_fp, 1)
    }
  }
}