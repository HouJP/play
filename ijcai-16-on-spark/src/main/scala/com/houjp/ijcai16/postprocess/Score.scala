package com.houjp.ijcai16.postprocess

import com.houjp.ijcai16
import com.houjp.ijcai16.datastructure.{Ans, MerchantInfo}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object Score {

  /** command line parameters */
  case class Params(user_ans_fp: String = "",
                    std_ans_fp: String = "",
                    merchant_info_fp: String = "")

  /** Parse the command line parameters */
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("BDA") {
      head("BDA", "1.0")
      opt[String]("user_ans_fp")
        .required()
        .text("The path of user ans file")
        .action { (x, c) => c.copy(user_ans_fp = x) }
      opt[String]("std_ans_fp")
        .required()
        .text("The path of standard ans file")
        .action { (x, c) => c.copy(std_ans_fp = x) }
      opt[String]("merchant_info_fp")
        .required()
        .text("The path of merchant info file")
        .action { (x, c) => c.copy(merchant_info_fp = x) }
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params) = {
    val conf = new SparkConf()
      .setAppName("BDA")
      .set("spark.hadoop.validateOutputSpecs", "false")
    if (ijcai16.is_local) {
      conf.setMaster("local[4]")
    }
    val sc = new SparkContext(conf)

    val merchant_info = MerchantInfo.load(sc, params.merchant_info_fp).map {
      e =>
        (e.merchant_id, e.budge)
    }

    val user_ans = Ans.load(sc, params.user_ans_fp).flatMap {
      e =>
        e.merchant_id_list.map {
          merchant_id =>
            (e.user_id, e.location_id, merchant_id)
        }
    }.distinct()
    val std_ans = Ans.load(sc, params.std_ans_fp).flatMap {
      e =>
        e.merchant_id_list.map {
          merchant_id =>
            (e.user_id, e.location_id, merchant_id)
        }
    }.distinct()

    val join_ans = user_ans.intersection(std_ans)

    val numerator: Double = join_ans.map(e => (e._3, 1)).reduceByKey(_+_).join(merchant_info).map {
      case (merchant_id: String, (cnt: Int, budget: Int)) =>
        (merchant_id, math.min(cnt, budget))
    }.map(_._2).sum()
    val p_denominator = user_ans.count().toDouble
    val r_denominator = std_ans.map(e => (e._3, 1)).reduceByKey(_+_).join(merchant_info).map {
      case (merchant_id: String, (cnt: Int, budget: Int)) =>
        (merchant_id, math.min(cnt, budget))
    }.map(_._2).sum()

    println(s"[INFO] numerator=$numerator, denominator(p)=$p_denominator, denominator(r)=$r_denominator")

    val p = numerator / p_denominator
    val r = numerator / r_denominator
    val f1 = 2 * p * r / (p + r)

    println(s"[INFO] p=$p, r=$r, f1=$f1")
  }
}