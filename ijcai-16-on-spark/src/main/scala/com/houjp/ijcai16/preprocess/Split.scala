package com.houjp.ijcai16.preprocess

import com.houjp.ijcai16.datastructure.{Taobao, KoubeiTrain}
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object Split {

  /** command line parameters */
  case class Params(data_pt: String = "")

  /** Parse the command line parameters */
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("BDA") {
      head("BDA", "1.0")
      opt[String]("data_pt")
        .required()
        .text("The path of data file")
        .action { (x, c) => c.copy(data_pt = x) }
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params) = {
    val sparkConf = new SparkConf()
      .setAppName("BDA")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(sparkConf)

    // load data from disk
    val koubei_train = KoubeiTrain.load(sc, params.data_pt + "/data-sets/ijcai2016_koubei_train")
    val taobao = Taobao.load(sc, params.data_pt + "/data-sets/ijcai2016_taobao")

    // random split user by 1:1
    val Array(u1, u2) = koubei_train.map(_.user_id).distinct().randomSplit(Array(0.5, 0.5))

    // split koubei_train
    val koubei_train_1 = koubei_train.map(e => (e.user_id, e)).join(u1.map(e => (e, 1))).map(_._2._1)
    val koubei_train_2 = koubei_train.map(e => (e.user_id, e)).join(u2.map(e => (e, 1))).map(_._2._1)

    // split taobao
    val taobao_1 = taobao.map(e => (e.user_id, e)).join(u1.map(e => (e, 1))).map(_._2._1)
    val taobao_2 = taobao.map(e => (e.user_id, e)).join(u2.map(e => (e, 1))).map(_._2._1)

    // save on disk
    u1.saveAsTextFile(params.data_pt + "/data-sets-split/user_p1")
    u2.saveAsTextFile(params.data_pt + "/data-sets-split/user_p2")
    koubei_train_1.saveAsTextFile(params.data_pt + "/data-sets-split/ijcai2016_koubei_train_p1")
    koubei_train_2.saveAsTextFile(params.data_pt + "/data-sets-split/ijcai2016_koubei_train_p2")
    taobao_1.saveAsTextFile(params.data_pt + "/data-sets-split/ijcai2016_taobao_p1")
    taobao_2.saveAsTextFile(params.data_pt + "/data-sets-split/ijcai2016_taobao_p2")

    println(s"[INFO] n(koubei_train)=${koubei_train.count}")
    println(s"[INFO] n(taobao)=${taobao.count}")

    println(s"[INFO] n(user)=${koubei_train.map(_.user_id).distinct().count}")
    println(s"[INFO] n(u1)=${u1.count}")
    println(s"[INFO] n(u2)=${u2.count}")

    println(s"[INFO] n(koubei_train_1)=${koubei_train_1.count}")
    println(s"[INFO] n(koubei_train_2)=${koubei_train_2.count}")

    println(s"[INFO] n(taobao_1)=${taobao_1.count}")
    println(s"[INFO] n(taobao_2)=${taobao_2.count}")
  }
}