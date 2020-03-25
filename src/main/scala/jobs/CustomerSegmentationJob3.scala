package jobs

import com.twitter.scalding.Args
import common.Environment
import org.apache.spark.sql.types._
import common.Common._
import datasources.DataSource
import org.apache.spark.sql.functions._
import utils.CosineSimilarity


object CustomerSegmentationJob3 {

  def main(args: Array[String]): Unit = {

    val params = Args(args)

    implicit val sc = Environment.sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val inputPath = params.required("input")
    val writePath = params.required("output")

    val transactions = DataSource.getTSVDataFrameWithSchema(inputPath, schema)

    val perPurchase = transactions.groupBy(INVOICE_NO, DATE_TIME, CUSTOMER_ID, COUNTRY)
      .agg(sum(col(PRICE) * col(QUANTITY)) as PRICE_PER_PURCHASE,
        countDistinct(STOCK_CODE) as PRODUCTS_PER_PURCHASE,
        sum(QUANTITY) as QUANTITY_PER_PURCHASE)

    val avgPerPurchase = perPurchase.agg(avg(PRICE_PER_PURCHASE) as AVG_PRICE_PER_PURCHASE,
      avg(PRODUCTS_PER_PURCHASE) as AVG_PRODUCTS_PER_PURCHASE,
      avg(QUANTITY_PER_PURCHASE) as AVG_QUANTITY_PER_PURCHASE)

    val perCustomer = perPurchase.groupBy(CUSTOMER_ID)
      .agg(count(ALL) as PURCHASES_PER_CUSTOMER,
        avg(PRICE_PER_PURCHASE) as AVG_PRICE_PER_PURCHASE_PER_CUSTOMER,
        avg(PRODUCTS_PER_PURCHASE) as AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER,
        avg(QUANTITY_PER_PURCHASE) as AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER)

    val avgPurchases = perCustomer.agg(avg(PURCHASES_PER_CUSTOMER) as AVG_PURCHASES_PER_CUSTOMER)


    val perCustomerWithAvg = perCustomer.crossJoin(avgPerPurchase).crossJoin(avgPurchases)
        .withColumn(PURCHASE_SCORE, {
          val scoreUDF = udf(scoreFunc)
          scoreUDF(col(PURCHASES_PER_CUSTOMER), col(AVG_PURCHASES_PER_CUSTOMER))
        })
      .withColumn(PRICE_SCORE, {
        val scoreUDF = udf(scoreFunc)
        scoreUDF(col(AVG_PRICE_PER_PURCHASE_PER_CUSTOMER), col(AVG_PRICE_PER_PURCHASE))
      })
      .withColumn(PRODUCTS_SCORE, {
        val scoreUDF = udf(scoreFunc)
        scoreUDF(col(AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER), col(AVG_PRODUCTS_PER_PURCHASE))
      })
      .withColumn(QUANTITY_SCORE, {
        val scoreUDF = udf(scoreFunc)
        scoreUDF(col(AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER), col(AVG_QUANTITY_PER_PURCHASE))
      })

    DataSource.saveDataFrameAsTSV(
      perCustomerWithAvg.repartition(1),
      writePath+"/perCustomerWithAvg",
      header = STR_BOOL_TRUE
    )

    val similarity = perCustomerWithAvg
      .withColumn(SIMILARITY, {
        val cosSinUDF = udf(cosSinFunc)
        cosSinUDF(array(col(PURCHASE_SCORE), col(PRICE_SCORE), col(PRODUCTS_SCORE), col(QUANTITY_SCORE)),
          array(lit(1), lit(1), lit(1), lit(1)))

      })

    DataSource.saveDataFrameAsTSV(
      similarity.repartition(1),
      writePath+"/similarity",
      header = STR_BOOL_TRUE
    )

    val minMaxSimilarity = similarity.select(SIMILARITY).agg(max(SIMILARITY) as MAX_SIMILARITY,
      min(SIMILARITY) as MIN_SIMILARITY)


    val segments = similarity.crossJoin(minMaxSimilarity)
      .withColumn(SEGMENT, {
        val segmentUDF = udf((sim: Double, min: Double, max: Double)=>{
          val scaledSim = scaleFunc(sim, min, max, 0.0, 100.0)
          val segment = getSegmentFunc(scaledSim)
          segment
        })
        segmentUDF(col(SIMILARITY), col(MIN_SIMILARITY), col(MAX_SIMILARITY))

      }).select(CUSTOMER_ID, PURCHASE_SCORE, PRICE_SCORE, PRODUCTS_SCORE, QUANTITY_SCORE, SIMILARITY, SEGMENT)


    val segmentStats = segments.groupBy(SEGMENT)
      .agg(count(ALL).as(NO_OF_CUSTOMERS),
        min(SIMILARITY),
        max(SIMILARITY),
        avg(SIMILARITY))



    DataSource.saveDataFrameAsTSV(
      segments.repartition(1),
      writePath+"/customer_segments3",
      header = STR_BOOL_TRUE
    )

    DataSource.saveDataFrameAsTSV(
      segmentStats.repartition(1),
      writePath+"/segment_statistics3",
      header = STR_BOOL_TRUE
    )

  }

  val INVOICE_NO = "InvoiceNo"
  val STOCK_CODE = "StockCode"
  val PRODUCT_NAME = "Description"
  val QUANTITY = "Quantity"
  val DATE_TIME = "InvoiceDate"
  val PRICE = "UnitPrice"
  val CUSTOMER_ID = "CustomerID"
  val COUNTRY = "Country"
  val ALL = "*"
  val SEGMENT = "segment"

  //Per Perchase-----------------------
  val PRICE_PER_PURCHASE = "pricePerPurchase"
  val PRODUCT_QUANTITY_RATIO_PER_PURCHASE = "productQuantityRatioPerPurchase"


  //Per Customer-----------------------
  val PURCHASES_PER_CUSTOMER = "purchasesPerCustomer"
  val AVG_PRICE_PER_PURCHASE_PER_CUSTOMER = "avgPricePerPurchasePerCustomer"
  val AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER = "avgProductQuantityRatioPerPurchasePerCustomer"


  //Avg -------------------------------
  val AVG_PURCHASES_PER_CUSTOMER = "avgPurchasesPerCustomer"
  val AVG_PRICE_PER_PURCHASE = "avgPricePerPurchase"
  val AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE = "avgProductQuantityRatioPerPurchase"


  val TOTAL_REVENUE = "totalRevenue"
  val TOTAL_PURCHASES = "totalPurchases"
  val TOTAL_CUSTOMERS = "totalCustomers"

  val NO_OF_CUSTOMERS = "noOfCustomers"


  val PRODUCTS_PER_PURCHASE = "productsPerPurchase"
  val QUANTITY_PER_PURCHASE = "quantityPerPurchase"

  val AVG_PRODUCTS_PER_PURCHASE = "avgProductsPerPurchase"
  val AVG_QUANTITY_PER_PURCHASE = "avgQuantityPerPurchase"

  val AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER = "avgProductsPerPurchasePerCustomer"
  val AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER = "avgQuantityPerPurchasePerCustomer"


  val SCALED_PURCHASES_PER_CUSTOMER = "scaled_purchasesPerCustomer"
  val SCALED_AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER = "scaled_avgProductsPerPurchasePerCustomer"
  val SCALED_AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER = "scaled_avgQuantityPerPurchasePerCustomer"
  val SCALED_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER = "scaled_avgPricePerPurchasePerCustomer"


  val MAX_PURCHASES_PER_CUSTOMER = "max_purchasesPerCustomer"
  val MIN_PURCHASES_PER_CUSTOMER = "min_purchasesPerCustomer"

  val MAX_AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER = "max_avgProductsPerPurchasePerCustomer"
  val MIN_AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER = "min_avgProductsPerPurchasePerCustomer"

  val MAX_AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER = "max_avgQuantityPerPurchasePerCustomer"
  val MIN_AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER = "min_avgQuantityPerPurchasePerCustomer"

  val MAX_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER = "max_avgPricePerPurchasePerCustomer"
  val MIN_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER = "min_avgPricePerPurchasePerCustomer"

  val SIMILARITY = "similarity"
  val MAX_SIMILARITY = "max_similarity"
  val MIN_SIMILARITY = "min_similarity"

  val PURCHASE_SCORE = "purchaseScore"
  val PRICE_SCORE = "priceScore"
  val PRODUCTS_SCORE = "productsScore"
  val QUANTITY_SCORE = "quantityScore"


  val schema = StructType(Array(
    StructField(INVOICE_NO, StringType, true),
    StructField(STOCK_CODE, StringType, true),
    StructField(PRODUCT_NAME, StringType, true),
    StructField(QUANTITY, IntegerType, true),
    StructField(DATE_TIME, StringType, true),
    StructField(PRICE, FloatType, true),
    StructField(CUSTOMER_ID, StringType, true),
    StructField(COUNTRY, StringType, true)
  ))


  val getSegmentFunc = (value : Double) => {
    if(value < 25){
      "1"
    }else if(value <= 50){
      "2"
    }else if(value < 75){
      "3"
    }else{
      "4"
    }
  }

  val scoreFunc : (Double, Double) => Int = (value: Double, avg: Double) =>{
    if(value < avg) 0 else 1
  }

  val cosSinFunc : (Seq[Int], Seq[Int]) => Double = (xs: Seq[Int], ys: Seq[Int]) =>{
    CosineSimilarity.cosineSimilarity(xs.toArray, ys.toArray)
  }

}
