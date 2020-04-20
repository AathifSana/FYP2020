package jobs.test

import com.twitter.scalding.Args
import common.Common.{COMMA, jaccardSimilarityFunc}
import common.Environment
import datasources.DataSource
import org.apache.spark.sql.functions.{col, lit, split, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Calculates the Jaccard similarity of the recommendations
  */
object JaccardSimilarityTestJob {

  def main(args: Array[String]): Unit = {

    val params = Args(args)

    implicit val sc = Environment.sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val inputPath1 = params.required("input1")
    val inputPath2 = params.required("input2")
    val outputPath = params.required("output")

    val df1 = DataSource.getTSVDataFrameWithSchema(inputPath1, schema1)
      .withColumn(VAL1, split(col(VAL1), COMMA))

    val df2 = DataSource.getTSVDataFrameWithSchema(inputPath2, schema2)
      .withColumn(VAL2, split(col(VAL2), COMMA))

    val joined = df1.join(df2, KEY)

    val jSimilar = joined.withColumn(JACCARD_SIM, {
      val JSim_udf = udf(jaccardSimilarityFunc)
      JSim_udf(col(VAL1), col(VAL2))
    })

    val jDistance = jSimilar.withColumn(JACCARD_DIS, lit(1.0) - col(JACCARD_SIM))

    val outDf = jDistance.select(KEY,JACCARD_SIM,JACCARD_DIS)

    DataSource.saveDataFrameAsTSV(outDf,outputPath)
  }

    val KEY = "KEY"
    val VAL1 = "VAL1"
    val VAL2 = "VAL2"
    val JACCARD_SIM = "jaccard_similarity"
    val JACCARD_DIS = "jaccard_distance"

  val schema1 = StructType(Array(
    StructField(KEY, StringType, true),
    StructField(VAL1, StringType, true)
  ))

  val schema2 = StructType(Array(
    StructField(KEY, StringType, true),
    StructField(VAL2, StringType, true)
  ))


}
