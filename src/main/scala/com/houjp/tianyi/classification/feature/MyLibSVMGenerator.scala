package com.houjp.tianyi.classification.feature

import com.houjp.tianyi
import com.houjp.tianyi.datastructure.RawPoint
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object MyLibSVMGenerator {

  /** command line parameters */
  case class Params(vvd_fp: String = tianyi.project_pt + "/data/raw/video-visit-data.txt.small",
                    out_fp: String = tianyi.project_pt + "/data/fs/mylibsvm_user-vt-first_user-vt-last_6_5.txt",
                    fs_fp: String = tianyi.project_pt + "/data/fs/user-vt-first_user-vt-last_6_5.txt",
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
      opt[String]("fs_fp")
        .text("")
        .action { (x, c) => c.copy(fs_fp = x) }
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
      .setAppName(s"tianyi-final user-vt-first")
      .set("spark.hadoop.validateOutputSpecs", "false")
    if (tianyi.is_local) {
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)

    val vvd = RawPoint.read(sc, p.vvd_fp, Int.MaxValue)
    val label_0 = CandidateGenerator.run(vvd, p.t_wid, p.w_len).map((_, 0))
    val label_1 = vvd.filter(_.wid == p.t_wid).map(_.uid).distinct().map((_, 1))

    val label = label_0.leftOuterJoin(label_1).map {
      e =>
        (e._1, e._2._2.getOrElse(e._2._1))
    }

    val fs = sc.textFile(p.fs_fp).map {
      line =>
        val Array(uid, fs) = line.split("\t")
        (uid, fs.split(",").zipWithIndex.map(e => s"${e._2 + 1}:${e._1}").mkString(" "))
    }

    label.join(fs).map {
      e =>
        s"${e._1}\t${e._2._1}\t${e._2._2}"
    }.saveAsTextFile(p.out_fp)
  }
}