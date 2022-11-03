package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

/** Compute the total amount spent per customer in some fake e-commerce data. */
object TotalSpentByCustomerSortedDataset_David {

  //make sure this matches your schema command later on.
  case class CustomerOrders(custId: Int, itemId: Int, itemSpent: Double)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("TotalSpentByCustomer")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    // Create schema when reading customer-orders
    val customerOrderSchema = new StructType().add("custId", IntegerType, nullable = true).add("itemId", IntegerType, nullable = true).add("itemSpent", DoubleType)


    // Load up the data into spark dataset
    // Use default separator (,), load schema from customerOrdersSchema and force case class to read it as dataset
    import spark.implicits._
    val customerDataSet = spark.read.schema(customerOrderSchema).csv("data/customer-orders.csv").as[CustomerOrders]

    //Now add up total, rounding things to 2, and call it "total spent"
    val customerSpending = customerDataSet.groupBy("custId").agg(round(sum("itemSpent"), 2).alias("totalSpentByCustomer"))
    val itemSpending = customerDataSet.groupBy("itemId").agg(round(sum("itemSpent"), 2).alias("totalSpentOnItem"))

    //customerSpending.collect().foreach(println)

    val customerSpendingTotals = customerSpending.sort("custId")
    val itemSpendingTotals = itemSpending.sort("itemId")

    customerSpendingTotals.show(15, customerSpending.count.toInt)
    Thread.sleep(5000)
    itemSpendingTotals.show(15,itemSpending.count.toInt)
    spark.stop()
  }
}

