package com.houjp.tianyi.classification.feature

import com.houjp.common.FeatureOpts
import com.houjp.tianyi
import com.houjp.tianyi.datastructure.{UBDPoint, RawPoint}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object L115DampedSum {
  /** command line parameters */
  case class Params(vvd_fp: String = tianyi.project_pt + "/data/raw/video-visit-data.txt.small",
                    ubd_fp: String = tianyi.project_pt + "/data/raw/user-behavior-data.small",
                    label_fp: String = tianyi.project_pt + "/data/stat/label_l1_index",
                    out_fp: String = tianyi.project_pt + "/data/fs/l1-15-damped-sum_6_5.txt",
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
      .setAppName(s"tianyi-final damped-sum")
      .set("spark.hadoop.validateOutputSpecs", "false")
    if (tianyi.is_local) {
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)

    val f_len = 1
    val vvd = RawPoint.read(sc, p.vvd_fp, Int.MaxValue)
    val cdd: RDD[(String, Array[Double])] = CandidateGenerator.run(vvd, p.t_wid, p.w_len).map((_, Array.fill[Double](f_len)(0.0)))

    val ubd = UBDPoint.read(sc, p.ubd_fp, p.label_fp, Int.MaxValue).filter(_.l1 == 15)

    val fs = cal(ubd, p.t_wid - p.w_len, p.t_wid - 1, p.t_wid + 2).map {
      case (uid: String, v: Double) =>
        (uid, Array(v))
    }

    val fs_all = cdd.leftOuterJoin(fs).map {
      e =>
        (e._1, e._2._2.getOrElse(e._2._1))
    }

    FeatureOpts.save(fs_all, p.out_fp)
  }

  def calByDayBySite(data: RDD[UBDPoint], beg_w: Int, end_w: Int, aim_w: Int):  RDD[(String, Array[Array[Double]])] = {
    // cal
    val is_normalize = false
    val denominator: Double = is_normalize match {
      case true =>
        Range(beg_w, end_w + 1).map { v =>
          math.pow(math.E, v - aim_w)
        }.sum
      case false => 1.0
    }
    //println(s"denominator=$denominator")

    data.filter(ele => (ele.wid >= beg_w) && (ele.wid <= end_w)).map { ele =>
      ((ele.uid, ele.did, ele.l1), (ele.wid, ele.vcnt))
    }.combineByKey[Double](
      (v: (Int, Int)) => math.pow(math.E, v._1 - aim_w) * v._2 / denominator,
      (c: Double, v: (Int, Int)) => c + math.pow(math.E,  v._1 - aim_w) * v._2 / denominator,
      (c1: Double, c2: Double) => c1 + c2).map { case ((uid: String, d: Int, v: Int), cnt: Double) =>
      (uid, Array(((d, v), cnt)))
    }.reduceByKey(_++_).map { case (uid: String, arr) =>
      val rec = new Array[Array[Double]](7)
      for (i <- 0 until  7) {
        rec(i) = new Array[Double](17)
      }
      for (i <- 0 until 7) {
        for (j <- 0 until 17) {
          rec(i)(j) = 0
        }
      }

      arr.foreach { case ((d: Int, v: Int), cnt: Double) =>
        rec(d - 1)(v) = cnt
      }

      val sum = rec.map(_.sum).sum

      (uid, rec, sum)
    }.filter(_._3 > 1e-8).map { case (uid: String, rec: Array[Array[Double]], sum: Double) =>
      (uid, rec)
    }
  }

  def cal(data: RDD[UBDPoint], beg_w: Int, end_w: Int, aim_w: Int):  RDD[(String, Double)] = {
    calByDayBySite(data, beg_w, end_w, aim_w).map(e => (e._1, e._2.map(_.sum).sum))
  }

}