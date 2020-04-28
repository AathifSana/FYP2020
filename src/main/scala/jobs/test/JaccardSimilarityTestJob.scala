package jobs.test

import com.twitter.scalding.Args
import common.Common.{COMMA, jaccardSimilarityFunc, STR_BOOL_TRUE}
import common.Environment
import datasources.DataSource
import org.apache.spark.sql.functions._
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
      .select(KEY,JACCARD_SIM,JACCARD_DIS)

    val outDf = jDistance.agg(avg(JACCARD_DIS) as JACCARD_DIS, avg(JACCARD_SIM) as JACCARD_SIM)

    DataSource.saveDataFrameAsTSV(outDf,outputPath, header = STR_BOOL_TRUE)
    DataSource.saveDataFrameAsTSV(jDistance,outputPath+"-debug", header = STR_BOOL_TRUE)

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
