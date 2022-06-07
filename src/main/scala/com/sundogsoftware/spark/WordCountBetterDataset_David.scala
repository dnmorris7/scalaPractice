package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Count up how many of each word occurs in a book, using regular expressions. */
object WordCountBetterDataset_David {

  case class Book(value: String)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine

    // Read each line of my book into an DatasetWordCountBetterDataset$


    // Split using a regular expression that extracts words

    // Normalize everything to lowercase

    // Count up the occurrences of each word

    // Show the results.

  }
}

