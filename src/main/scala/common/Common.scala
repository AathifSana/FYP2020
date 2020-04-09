package common

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.Vectors

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


  val meanSquaredDifferenceFunc = (xs: Seq[Double], ys: Seq[Double]) => {
    ((xs zip ys).map { case (x,y) => math.pow(y - x, 2) }.sum) / xs.length
  }

  val meanSquaredSimilarityFunc = (xs: Seq[Double], ys: Seq[Double]) => {
    1 / (meanSquaredDifferenceFunc(xs,ys) + 1)
  }

  val meanSquaredDifferenceUDF = udf(meanSquaredDifferenceFunc)
  val meanSquaredSimilarityUDF = udf(meanSquaredSimilarityFunc)

  val jaccardSimilarityFunc = (seq1: Seq[String], seq2: Seq[String]) => {

    val set1 = seq1.toSet
    val set2 = seq2.toSet

    if (set1.isEmpty || set2.isEmpty){
      0.0
    }else{
      set1.intersect(set2).size/set1.union(set2).size.toDouble
    }
  }

}
