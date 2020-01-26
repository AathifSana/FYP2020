package common

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.udf


object Constants {

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
  val validateIsNumbersfunc: (String => Boolean) = (arg: String) => {
    arg != null && arg.nonEmpty && arg.matches(numberFilterRegex)
  }

  val atLeastOneNumberUDF = (code: String)=> {
    code != null && code != StringUtils.EMPTY && code.exists(_.isDigit)
  }
}
