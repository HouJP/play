package com.houjp.tianyi.classification.feature

import com.houjp.common.FeatureOpts
import com.houjp.tianyi
import com.houjp.tianyi.datastructure.RawPoint
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object ContinueMinDis {

  /** command line parameters */
  case class Params(vvd_fp: String = tianyi.project_pt + "/data/raw/video-visit-data.txt.small",
                    out_fp: String = tianyi.project_pt + "/data/fs/continue-min-dis_6_5.txt",
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
      .setAppName(s"tianyi-final hour-gcnt")
      .set("spark.hadoop.validateOutputSpecs", "false")
    if (tianyi.is_local) {
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)

    val f_len = 10
    val vvd = RawPoint.load(sc, p.vvd_fp, Int.MaxValue)
    val cdd = CandidateGenerator.run(vvd, p.t_wid, p.w_len).map((_, Array.fill[Double](f_len)(0.0)))

    val fs = RawPoint.filter(vvd, p.t_wid, p.w_len).map {
      e =>
        val n = (60 - e.mid) +
          (23 - e.hid) * 60 +
          (7 - e.did) * 24 * 60 +
          (p.t_wid - e.wid - 1) * 7 * 24 * 60

        (e.uid, e.vid, n)
    }.distinct().map {
      case (uid: String, vid: Int, n: Int) =>
        //println(s"uid=$uid, vid=$vid, n=$n")
        ((uid, vid), Array(n))
    }.reduceByKey(_++_).map {
      case ((uid: String, vid: Int), arr: Array[Int]) =>
        val arr_sorted: Array[Int] = arr.sorted
        var ans = 30
        var add = 30
        Range(1, arr_sorted.length).foreach {
          id =>
            if (arr_sorted(id) - arr_sorted(id - 1) == 30) {
              add += 30
            } else {
              ans = math.max(ans, add)
              add = 30
            }
        }
        ans = math.max(ans, add)
        (uid, Array((vid, ans / 30)))
    }.reduceByKey(_++_).map {
      case (uid: String, arr: Array[(Int, Int)]) =>
        val fs = Array.fill[Double](10)(0.0)
        arr.foreach {
          case (vid: Int, n: Int) =>
            fs(vid - 1) = n.toDouble
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