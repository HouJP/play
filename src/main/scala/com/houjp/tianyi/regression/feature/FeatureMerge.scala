package com.houjp.tianyi.regression.feature

import com.houjp.tianyi
import com.houjp.common.FeatureOpts
import com.houjp.tianyi.datastructure.{UBDPoint, RawPoint}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser


object FeatureMerge {

  /** command line parameters */
  case class Params(gty_fp: String = tianyi.project_pt + "/data/raw/video-visit-data.txt.small",
                    hjp_fp: String = tianyi.project_pt + "/data/raw/user-behavior-data.small",
                    out_fp: String = tianyi.project_pt)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)
    val default_params = Params()

    val parser = new OptionParser[Params]("") {
      head("", "1.0")
      opt[String]("gty_fp")
        .text("")
        .action { (x, c) => c.copy(gty_fp = x) }
      opt[String]("hjp_fp")
        .text("")
        .action { (x, c) => c.copy(hjp_fp = x) }
      opt[String]("out_fp")
        .text("")
        .action { (x, c) => c.copy(out_fp = x) }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params): Unit = {
    val conf = new SparkConf()
      .setAppName(s"tianyi-final l1-label-visit-day-count")
      .set("spark.hadoop.validateOutputSpecs", "false")
    if (tianyi.is_local) {
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)


    val gty_fs = sc.textFile(p.gty_fp).map {
      line =>
        val subs = line.split("\t")
        (subs(0), subs.slice(1, subs.length).map(_.toDouble))
    }
    val hjp_fs = sc.textFile(p.hjp_fp).map {
      line =>
        val Array(uid, fs_s) = line.split("\t")
        (uid, fs_s.split(",").map(_.toDouble))
    }
    val hjp_fs_len = hjp_fs.take(1)(0)._2.length

    val fs_all = gty_fs.leftOuterJoin(hjp_fs).map {
      case (uid: String, (fs_1: Array[Double], fs_2: Option[Array[Double]])) =>
        val new_fs_2 = fs_2.getOrElse(Array.fill[Double](hjp_fs_len)(0.0))
        (uid, fs_1 ++ new_fs_2)
    }
    val fs_all_len = fs_all.take(1)(0)._2.length

    println(s"HouJP >> hjp_fs_len=$hjp_fs_len, fs_all_len=$fs_all_len")

    FeatureOpts.save(fs_all, p.out_fp)
  }
}