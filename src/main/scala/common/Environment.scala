package common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Environment {

  var isTestEnv: Boolean = false

  def turnOnTestEnv() = {
    isTestEnv = true
  }

  lazy val conf = new SparkConf().setAppName(APP_NM)

  lazy val sparkSession = isTestEnv match {

    case true => SparkSession.builder()
      .master(LOCAL)
      .appName(APP_NM)
      .enableHiveSupport()
      .getOrCreate()

    case false => SparkSession.builder()
      .config(conf)
      .appName(APP_NM)
      .enableHiveSupport()
      .getOrCreate()
  }

  var APP_NM = "SparkJob"
  val LOCAL = "local"
}
