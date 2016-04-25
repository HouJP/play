package com.houjp.ijcai16.preprocess

import com.houjp.ijcai16
import com.houjp.ijcai16.datastructure.{Ans, KoubeiTrain}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object STDAnsGenerator {

  /** command line parameters */
  case class Params(input_fp: String = "",
                    output_fp: String = "",
                    month_id: Int = 11)

  /** Parse the command line parameters */
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("BDA") {
      head("BDA", "1.0")
      opt[String]("input_fp")
        .required()
        .text("The path of input data file")
        .action { (x, c) => c.copy(input_fp = x) }
      opt[String]("output_fp")
        .required()
        .text("The path of output data file")
        .action { (x, c) => c.copy(output_fp = x) }
      opt[Int]("month_id")
        .required()
        .text("The id of month")
        .action { (x, c) => c.copy(month_id = x) }
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

    val tuple = KoubeiTrain.load(sc, params.input_fp).filter(e => e.month == params.month_id).map {
      e =>
        (e.user_id, e.merchant_id, e.location_id)
    }.distinct()

    val ans = Ans.tupleToAns(tuple)

    ans.saveAsTextFile(params.output_fp)
  }
}