package common

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions._

object Common {

  //values and characters
  val BLANK = ""
  val SPACE = " "
  val COMMA = ","
  val UNDERSCORE = "_"
  val TAB = "\t"
  val PIPE = '|'
  val ZERO = 0
  val TRUE = 1
  val FALSE = 0
  val STR_BOOL_TRUE = "true"
  val STR_BOOL_FALSE = "false"

  //formats
  val PARQUET_FORMAT = "parquet"
  val CSV_FORMAT = "com.databricks.spark.csv"

  //regex
  val numberFilterRegex = "^[0-9]*$"

  //udf functions
  val validateIsNumbersUDF = udf((arg: String) => {
    arg != null && arg.nonEmpty && arg.matches(numberFilterRegex)
  })

  val atLeastOneNumberUDF = udf((arg: String)=> {
    arg != null && arg != StringUtils.EMPTY && arg.exists(_.isDigit)
  })

  val scaleFunc = (input: Double, min: Double, max: Double, rangeMin: Double, rangeMax: Double) => {
    ((input - min) / (max - min) * (rangeMax - rangeMin)) + rangeMin
  }
  val scaleUDF = udf(scaleFunc)


  val euclideanDistanceFunc = (xs: Seq[Double], ys: Seq[Double]) => {
    math.sqrt((xs zip ys).map { case (x,y) => math.pow(y - x, 2) }.sum)
  }

  val euclideanDistanceUDF = udf(euclideanDistanceFunc)


}
