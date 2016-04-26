package com.houjp.ijcai16.preprocess

import com.houjp.ijcai16
import com.houjp.ijcai16.datastructure.{Ans, KoubeiTrain}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object STDAnsGenerator {

  /** command line parameters */
  case class Params(train_fp: String = "",
                    filter_fp: String = "",
                    output_fp: String = "",
                    month_sid: Int = 11,
                    month_len: Int = 1)

  /** Parse the command line parameters */
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("BDA") {
      head("BDA", "1.0")
      opt[String]("train_fp")
        .required()
        .text("The path of training data file")
        .action { (x, c) => c.copy(train_fp = x) }
      opt[String]("filter_fp")
        .required()
        .text("The path of filter file")
        .action { (x, c) => c.copy(filter_fp = x) }
      opt[String]("output_fp")
        .required()
        .text("The path of output data file")
        .action { (x, c) => c.copy(output_fp = x) }
      opt[Int]("month_sid")
        .required()
        .text("The start id of month")
        .action { (x, c) => c.copy(month_sid = x) }
      opt[Int]("month_len")
        .required()
        .text("The len of month")
        .action { (x, c) => c.copy(month_len = x) }
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

    val tuple = KoubeiTrain.load(sc, params.train_fp).filter(e => (e.month >= params.month_sid) && (e.month <= params.month_sid + params.month_len - 1)).map {
      e =>
        (e.user_id, e.merchant_id, e.location_id)
    }.distinct()

    val filter = sc.textFile(params.filter_fp).map {
      s =>
        val Array(user_id, location_id) = s.split(",")
        ((user_id, location_id), 1)
    }

    val tuple_filtered = tuple.map {
      case (user_id: String, merchant_id: String, location_id: String) =>
        ((user_id, location_id), merchant_id)
    }.join(filter).map {
      case ((user_id: String, location_id: String), (merchant_id: String, _)) =>
        (user_id, merchant_id, location_id)
    }

    val ans = Ans.tupleToAns(tuple_filtered)

    ans.saveAsTextFile(params.output_fp)
  }
}