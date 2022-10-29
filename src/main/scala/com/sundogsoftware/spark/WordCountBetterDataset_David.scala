package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Count up how many of each word occurs in a book, using regular expressions. */
object WordCountBetterDataset_David {

  case class Book(value: String)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
   // val sc = new SparkContext("local[*]", "AppName")

    // Create a SparkSession using every core of the local machine


   val session =  SparkSession.builder().appName("WordCount").master("local[*]").config("spark.sql.warehouse.dir", "local").getOrCreate()

    // Read each line of my book into a dataset
    //val data = session.read.text(("data/book.txt")).as("Book")
    import session.implicits._

    val data = session.read.text("data/book.txt").as[Book]
    val words=   data.select(explode(split($"value", "\\W+")).alias("word")).filter($"word"=!="")

    words.show()


    //val words =    data.select(explode(split($"Book", "\\W+"))).alias("word")

    // Split using a regular expression that extracts words
  //data.select(explode("word", "\\W+"))


    // Normalize everything to lowercase
  val lowercaseWords=words.select(lower($"word").alias("word"))

    // Count up the occurrences of each word
val wordsCount = lowercaseWords.groupBy("word").count()

    val wordCountsSorted=wordsCount.sort("count")

    // Show the results.
    wordCountsSorted.show(wordCountsSorted.count.toInt)

  }
}

