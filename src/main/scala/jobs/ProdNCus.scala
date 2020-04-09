package jobs

import com.twitter.scalding.Args
import common.Common.STR_BOOL_TRUE
import common.Environment
import datasources.DataSource
import jobs.DataPreprocessingJob._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType}

object ProdNCus {

  def main(args: Array[String]): Unit = {

    val params = Args(args)

    implicit val sc = Environment.sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val inputPath = params.required("input")
    val writePath = params.required("output")

    val transactions = DataSource.getTSVDataFrame(inputPath, header = STR_BOOL_TRUE)
                         .withColumn(QUANTITY, col(QUANTITY).cast(IntegerType))
                          .withColumn(PRICE, col(PRICE).cast(FloatType))
      .filter(
        col(STOCK_CODE).isNotNull && col(STOCK_CODE) =!= StringUtils.EMPTY && col(PRICE) > 0
         && col(CUSTOMER_ID).isNotNull && col(CUSTOMER_ID) =!= StringUtils.EMPTY

      )

    val mostFreqPName = udf { list: Seq[Row] =>
      list.maxBy(_.getLong(1)).getString(0)
    }

    val productNames = transactions.groupBy(STOCK_CODE, PRODUCT_NAME).count()
      .groupBy(STOCK_CODE).agg(mostFreqPName(collect_list(struct(PRODUCT_NAME, COUNT))) as PRODUCT_NAME)


    val mostFreqPrice = udf { list: Seq[Row] =>
      list.maxBy(_.getLong(1)).getFloat(0)
    }

    val productPrices = transactions.groupBy(STOCK_CODE, PRICE).count()
      .groupBy(STOCK_CODE).agg(mostFreqPrice(collect_list(struct(PRICE, COUNT))) as PRICE)


    val products = transactions.drop(PRODUCT_NAME, PRICE)
      .join(productNames, STOCK_CODE)
      .join(productPrices, STOCK_CODE)
      .select(STOCK_CODE,PRODUCT_NAME,PRICE).distinct()

    DataSource.saveDataFrameAsTSV(
      products.repartition(1),
      writePath + "/products"
    )

    val customers = transactions.select(CUSTOMER_ID).distinct().withColumn("segment", lit("null"))

    DataSource.saveDataFrameAsTSV(
      customers.repartition(1),
      writePath + "/customers"
    )

  }
}
