package com.houjp.tianyi.classification.feature

import com.houjp.common.FeatureOpts
import com.houjp.tianyi
import com.houjp.tianyi.datastructure.{UBDPoint, RawPoint}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object L1LabelDayGCount {
  /** command line parameters */
  case class Params(vvd_fp: String = tianyi.project_pt + "/data/raw/video-visit-data.txt.small",
                    ubd_fp: String = tianyi.project_pt + "/data/raw/user-behavior-data.small",
                    label_fp: String = tianyi.project_pt + "/data/stat/label_l1_index",
                    out_fp: String = tianyi.project_pt + "/data/fs/l1-label-visit-day-gcnt_6_5.txt",
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
      .setAppName(s"tianyi-final l1-label-number")
      .set("spark.hadoop.validateOutputSpecs", "false")
    if (tianyi.is_local) {
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)

    val step = 5
    val base = Array(1.0, 2.0, 7.0, 14.0, 35.0)
    val f_len = 17 * step
    val vvd = RawPoint.load(sc, p.vvd_fp, Int.MaxValue)
    val cdd: RDD[(String, Array[Double])] = CandidateGenerator.run(vvd, p.t_wid, p.w_len).map((_, Array.fill[Double](f_len)(0.0)))

    val ubd = UBDPoint.read(sc, p.ubd_fp, p.label_fp, Int.MaxValue)
    val fs = UBDPoint.filter(ubd, p.t_wid, p.w_len).map {
      p =>
        (p.uid, p.wid, p.did, p.l1)
    }.distinct().map {
      case (uid: String, wid: Int, did: Int, l1: Int) =>
        val bias = (p.t_wid - wid - 1) * 7 + (8 - did)
        //println(s"wid=$wid,did=$did,l1=$l1,bias=$bias")
        (uid, Array((l1, bias)))
    }.reduceByKey(_++_).map {
      case (uid: String, arr: Array[(Int, Int)]) =>
        val v = Array.fill[Double](17 * step)(0.0)
        arr.foreach {
          case (l1, day) =>
            base.indices.foreach {
              case id =>
                if (day <= base(id)) {
                  v(l1 * step + id) += 1
                }
            }
        }
        (uid, v)
    }

    /**
      * (p.uid, p.l1)
      * }.distinct().map {
      * case (uid: String, l1: Int) =>
      * (uid, 1)
      * }.reduceByKey(_+_).map {
      * case (uid: String, n: Int) =>
      * (uid, Array[Double](n.toDouble))
      * }
*/
    val fs_all = cdd.leftOuterJoin(fs).map {
      e =>
        (e._1, e._2._2.getOrElse(e._2._1))
    }

    FeatureOpts.save(fs_all, p.out_fp)
  }
}