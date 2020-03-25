package jobs

import com.twitter.scalding.Args
import common.Environment
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types._
import common.Common._
import datasources.DataSource
import org.apache.spark.sql.functions._
import co.theasi.plotly._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler

import scala.collection.mutable.ArrayBuffer


object CustomerClusteringJob {

  def main(args: Array[String]): Unit = {

    val params = Args(args)

    implicit val sc = Environment.sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val inputPath = params.required("input")
    val writePath = params.required("output")

    val transactions = DataSource.getTSVDataFrameWithSchema(inputPath, schema)

    val perPerchase = transactions.groupBy(INVOICE_NO, DATE_TIME, CUSTOMER_ID, COUNTRY)
      .agg(sum(col(PRICE) * col(QUANTITY)) as PRICE_PER_PURCHASE,
        (countDistinct(STOCK_CODE) / sum(QUANTITY)) as PRODUCT_QUANTITY_RATIO_PER_PURCHASE)


    val perCustomer = perPerchase.groupBy(CUSTOMER_ID)
      .agg(count(ALL) as PURCHASES_PER_CUSTOMER,
        avg(PRICE_PER_PURCHASE) as AVG_PRICE_PER_PURCHASE_PER_CUSTOMER,
        avg(PRODUCT_QUANTITY_RATIO_PER_PURCHASE) as AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER)


    val minMaxPerCustomer = perCustomer.agg(
      min(PURCHASES_PER_CUSTOMER) as MIN_PURCHASES_PER_CUSTOMER
      , max(PURCHASES_PER_CUSTOMER) as MAX_PURCHASES_PER_CUSTOMER
      , min(AVG_PRICE_PER_PURCHASE_PER_CUSTOMER) as MIN_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER
      , max(AVG_PRICE_PER_PURCHASE_PER_CUSTOMER) as MAX_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER
      , min(AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER) as MIN_AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER
      , max(AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER) as MAX_AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER)


    val scaledPerCustomer = perCustomer.crossJoin(minMaxPerCustomer)
      .withColumn(PURCHASES_PER_CUSTOMER, scaleUDF(col(PURCHASES_PER_CUSTOMER),
        col(MIN_PURCHASES_PER_CUSTOMER), col(MAX_PURCHASES_PER_CUSTOMER), lit(1), lit(100)))
      .withColumn(AVG_PRICE_PER_PURCHASE_PER_CUSTOMER, scaleUDF(col(AVG_PRICE_PER_PURCHASE_PER_CUSTOMER),
        col(MIN_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER), col(MAX_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER), lit(1), lit(100)))
      .withColumn(AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER, scaleUDF(col(AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER),
        col(MIN_AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER), col(MAX_AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER), lit(1), lit(100)))

    val assembler = new VectorAssembler()
      .setInputCols(
        Array(
          PURCHASES_PER_CUSTOMER,
          AVG_PRICE_PER_PURCHASE_PER_CUSTOMER,
          AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER
    )).setOutputCol(FEATURES)

    val featureDf = assembler.transform(scaledPerCustomer).select(FEATURES)

    var costArray = ArrayBuffer[Double]()
    val clusters = (2 to 15)

    clusters.foreach{c =>
      val kmeans = new KMeans().setK(c).setFeaturesCol(FEATURES).setPredictionCol(SEGMENT)
      val model = kmeans.fit(featureDf)
      costArray += model.computeCost(featureDf)
    }


    //------------------------------------------------------------------------

    import co.theasi.plotly
    import util.Random


    val p = Plot().withScatter(clusters, costArray.toSeq)

    draw(p, "K means Elbow", writer.FileOptions(overwrite=true))

    // returns  PlotFile(pbugnion:173,basic-scatter)



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

  val MAX_PURCHASES_PER_CUSTOMER = "max_purchasesPerCustomer"
  val MIN_PURCHASES_PER_CUSTOMER = "min_purchasesPerCustomer"

  val MAX_AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER = "max_avgProductQuantityRatioPerPurchasePerCustomer"
  val MIN_AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER = "min_avgProductQuantityRatioPerPurchasePerCustomer"

  val MAX_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER = "max_avgPricePerPurchasePerCustomer"
  val MIN_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER = "min_avgPricePerPurchasePerCustomer"

  //Avg -------------------------------
  val AVG_PURCHASES_PER_CUSTOMER = "avgPurchasesPerCustomer"
  val AVG_PRICE_PER_PURCHASE = "avgPricePerPurchase"
  val AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE = "avgProductQuantityRatioPerPurchase"

  val FEATURES = "features"

  val TOTAL_REVENUE = "totalRevenue"
  val TOTAL_PURCHASES = "totalPurchases"
  val TOTAL_CUSTOMERS = "totalCustomers"

  val NO_OF_CUSTOMERS = "noOfCustomers"


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


  val getSegmentFunc : ((Double,Double,Double,Double,Double,Double) => String) =
    (purchasePerCustomer : Double, pricePerPurchase :Double, productQuantityRatio:Double,
     avgPurchasePerCustomer:Double,avgPricePerPurchase :Double, avgProductQuantityRatio:Double) => {

      var segment = StringUtils.EMPTY

      if (purchasePerCustomer >= avgPurchasePerCustomer && pricePerPurchase >= avgPricePerPurchase && productQuantityRatio < avgProductQuantityRatio){
        segment = "1"
      }else if(purchasePerCustomer >= avgPurchasePerCustomer && pricePerPurchase >= avgPricePerPurchase && productQuantityRatio >= avgProductQuantityRatio){
        segment = "2"
      }else if(purchasePerCustomer >= avgPurchasePerCustomer && pricePerPurchase < avgPricePerPurchase && productQuantityRatio < avgProductQuantityRatio){
        segment = "3"
      }else if(purchasePerCustomer >= avgPurchasePerCustomer && pricePerPurchase < avgPricePerPurchase && productQuantityRatio >= avgProductQuantityRatio){
        segment = "4"
      }else if(purchasePerCustomer < avgPurchasePerCustomer && pricePerPurchase >= avgPricePerPurchase && productQuantityRatio < avgProductQuantityRatio){
        segment = "5"
      }else if(purchasePerCustomer < avgPurchasePerCustomer && pricePerPurchase >= avgPricePerPurchase && productQuantityRatio >= avgProductQuantityRatio){
        segment = "6"
      }else if(purchasePerCustomer < avgPurchasePerCustomer && pricePerPurchase < avgPricePerPurchase && productQuantityRatio < avgProductQuantityRatio){
        segment = "7"
      }else if(purchasePerCustomer < avgPurchasePerCustomer && pricePerPurchase < avgPricePerPurchase && productQuantityRatio >= avgProductQuantityRatio){
        segment = "8"
      }

      segment
    }
}
