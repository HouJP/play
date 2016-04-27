package com.houjp.tianyi.regression.postprocess


import com.houjp.common.VectorOpts
import com.houjp.tianyi
import com.houjp.tianyi.datastructure.AnsPoint
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object Compare {
  /** command line parameters */
  case class Params(a1_fp: String = "",
                    a2_fp: String = "",
                    std_fp: String = "",
                    out_fp: String = "")


  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)
    val default_params = Params()

    val parser = new OptionParser[Params]("") {
      head("", "1.0")
      opt[String]("a1_fp")
        .text("")
        .action { (x, c) => c.copy(a1_fp = x) }
      opt[String]("a2_fp")
        .text("")
        .action { (x, c) => c.copy(a2_fp = x) }
      opt[String]("std_fp")
        .text("")
        .action { (x, c) => c.copy(std_fp = x) }
      opt[String]("out_fp")
        .text("")
        .action { (x, c) => c.copy(out_fp = x) }
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
      .setAppName(s"tianyi-final compare")
      .set("spark.hadoop.validateOutputSpecs", "false")
    if (tianyi.is_local) {
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)

    val a1 = AnsPoint.read(sc, p.a1_fp).map {
      case (uid: String, v: Array[Double]) =>
        val v_10 = Array.fill[Double](10)(0.0)
        Range(0, 70).foreach {
          id =>
            v_10(id % 10) += v(id)
        }
        (uid, v_10)
    }

    val a2 = AnsPoint.read(sc, p.a2_fp).map {
      case (uid: String, v: Array[Double]) =>
        val v_10 = Array.fill[Double](10)(0.0)
        Range(0, 70).foreach {
          id =>
            v_10(id % 10) += v(id)
        }
        (uid, v_10)
    }

    val std = AnsPoint.read(sc, p.std_fp).map {
      case (uid: String, v: Array[Double]) =>
        val v_10 = Array.fill[Double](10)(0.0)
        Range(0, 70).foreach {
          id =>
            v_10(id % 10) += v(id)
        }
        (uid, v_10)
    }

    val user = a1.map(_._1).intersection(a2.map(_._1)).intersection(std.map(_._1))
    println(s"[INFO] n(user)=${user.count}")

    val a1_cos = a1.join(std).map {
      case (uid: String, (v1: Array[Double], v2: Array[Double])) =>
        (uid, VectorOpts.calCosSimilarity(v1, v2))
    }

    val a2_cos = a2.join(std).map {
      case (uid: String, (v1: Array[Double], v2: Array[Double])) =>
        (uid, VectorOpts.calCosSimilarity(v1, v2))
    }

    a1_cos.join(a2_cos).map {
      case (uid: String, (c1: Double, c2: Double)) =>
        s"$uid,$c1,$c2"
    }.saveAsTextFile(p.out_fp)
  }
}