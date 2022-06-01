package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

object FriendsByAge_David {
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val session = SparkSession.builder.appName("FriendsByAge_David").master("local[*]").getOrCreate()
    //println("Spark Version: "+session.version)

    // Load each line of the source data into an Dataset
    val df = session.read.option("header", "true").option("inferSchema", "true").csv("data/fakefriends.csv")
    df.show()

    // Select only age and numFriends columns
   // df.select(col("_c1"), col("_c2"), col("_c3")).show()
    val resultSet= df.select("age", "name", "friends")
    resultSet.show()

    // From friendsByAge we group by "age" and then compute average

    val resultSet2=resultSet.groupBy("age").count()
    resultSet2.orderBy("age").show()

    val averages = resultSet.groupBy("age").avg("friends")


    // Sorted:
    averages.orderBy("age").show()
    // Formatted more nicely:
    val averagesFormatted = resultSet.groupBy("age").agg(round(avg("friends"), 2).alias("Friend's Averages"))
    averagesFormatted.orderBy("age").show()
    // With a custom column name:

    session.stop()

  }
}
