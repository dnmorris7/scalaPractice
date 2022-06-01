package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark._

object TotalSpentByCustomer_David {


  def parseLine(line: String): (Int, Double) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the age and numFriends fields, and convert to integers
    val id = fields(0).toInt
    val spending = fields(2).toDouble
    // Create a tuple that is our result.
    (id, spending)
  }

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local", "TotalSpentByCustomer_David")

    //take in the customer order data
    val input = sc.textFile("data/customer-orders.csv")

    val input2 = input.map(parseLine) //
    val custSpending = input2.reduceByKey((x, y) => x + y) //.foreach(println)
    // val custSpending = input2.reduceByKey((x, y) => sum(x+y))


    // Print the results.

    val results = custSpending.collect().sorted//.foreach(println)


    //flip the results
    val flipped = custSpending.map(x => (x._2, x._1))

    flipped.collect().sorted.foreach(println)


    /*
    *
    * Split each comma-delimited line into fields.
    * Map each line to key/value pairs of customer ID and dollar amount_spent
    *
    * use reduceByKey to add up amount spent by customer customerID
    * collect() and print results.
    *
    *
    *
(90,5290.41)
(91,4642.2603)
(92,5379.281)
(93,5265.75)
(94,4475.5703)
(95,4876.8394)
(96,3924.23)
(97,5977.1895)
(98,4297.26)
(99,4172.29)

    * */


  }
}