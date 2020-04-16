package jobs

import java.sql.Timestamp

import com.twitter.scalding.Args
import common.Common.STR_BOOL_TRUE
import common.Environment
import datasources.DataSource
import jobs.DataPreprocessingJob._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType}
import java.text.SimpleDateFormat

object ProdNCus {

  def main(args: Array[String]): Unit = {

    val params = Args(args)

    implicit val sc = Environment.sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val inputPath = params.required("input")
    val writePath = params.required("output")
    val pattern =  "MM/dd/yy hh:mm"

    implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
    }


    val transactions = DataSource.getTSVDataFrame(inputPath, header = STR_BOOL_TRUE)
                         .withColumn(QUANTITY, col(QUANTITY).cast(IntegerType))
                          .withColumn(PRICE, col(PRICE).cast(FloatType))
      .filter(
        col(STOCK_CODE).isNotNull && col(STOCK_CODE) =!= StringUtils.EMPTY && col(PRICE) > 0
         && col(CUSTOMER_ID).isNotNull && col(CUSTOMER_ID) =!= StringUtils.EMPTY
      )
      .withColumn(DATE_TIME, unix_timestamp(col(DATE_TIME), pattern).cast("timestamp"))
        .withColumn(DATE_TIME, {
          val timeUDF = udf { dt:Timestamp=>
            val datetime = dt match {
              case null => {
                val dateFormat = new SimpleDateFormat(pattern)
                val date = dateFormat.parse("12/1/2010 8:34")
                val time = date.getTime
                new Timestamp(time)
              }
              case _ => dt
            }
            datetime
          }
          timeUDF(col(DATE_TIME))
        })


    val LatestPName = udf { list: Seq[Row] =>
      list.sortBy(_.getTimestamp(1)).map(_.getString(0)).last
    }

    val productNames = transactions.groupBy(STOCK_CODE)
      .agg(LatestPName(collect_set(struct(PRODUCT_NAME, DATE_TIME))) as PRODUCT_NAME)



    val LatestPrice = udf { list: Seq[Row] =>
      list.sortBy(_.getTimestamp(1)).map(_.getFloat(0)).last
    }

    val productPrices = transactions.groupBy(STOCK_CODE)
      .agg(LatestPrice(collect_set(struct(PRICE, DATE_TIME))) as PRICE)


    val products = productNames.join(productPrices, STOCK_CODE)
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

    DataSource.saveDataFrameToDatabase(products, "products")
    DataSource.saveDataFrameToDatabase(customers, "customers")


  }
}
