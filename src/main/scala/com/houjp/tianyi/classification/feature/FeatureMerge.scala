package com.houjp.tianyi.classification.feature

import com.houjp.tianyi
import com.houjp.tianyi.classification.FeatureOpts
import com.houjp.tianyi.datastructure.RawPoint
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object FeatureMerge {

  /** command line parameters */
  case class Params(fs_pt: String = tianyi.project_pt + "/data/fs/",
                    fs_name: String = "user-vt-first_user-vt-last")

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)
    val default_params = Params()

    val parser = new OptionParser[Params]("") {
      head("", "1.0")
      opt[String]("fs_pt")
        .text("")
        .action { (x, c) => c.copy(fs_pt = x) }
      opt[String]("fs_name")
        .text("")
        .action { (x, c) => c.copy(fs_name = x) }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params): Unit = {
    val conf = new SparkConf()
      .setAppName(s"tianyi-final feature-merge")
      .set("spark.hadoop.validateOutputSpecs", "false")
    if (tianyi.is_local) {
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)

    val fs_arr = p.fs_name.split("_")

    val fs_fp = p.fs_pt + s"/${fs_arr(0)}.txt"
    var fs_all = FeatureOpts.load(sc, fs_fp)

    Range(1, fs_arr.length).foreach {
      id =>
        val fs_fp = p.fs_pt + s"${fs_arr(id)}.txt"
        fs_all = FeatureOpts.merge(fs_all, FeatureOpts.load(sc, fs_fp))
    }

    val out_fp = p.fs_pt + s"${p.fs_name}.txt"
    fs_all.map {
      case (uid: String, fs: String) =>
        s"$uid\t$fs"
    }.saveAsTextFile(out_fp)
  }

}