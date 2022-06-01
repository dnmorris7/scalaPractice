package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount_David {

  def main(Args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount_David")

    // Read each line of my book into an RDD
    val book = sc.textFile("data/book.txt")

    //try a regular map for comparission
    //val wordsMap = book.map(x => x.split(" ")).foreach(println)  //<- Just returns arrays and arrays. FlatMap deals with that sort of output

    // Split into words separated by a space character
    //val wordsFlatMap = book.flatMap(x => x.split(" ")) //.foreach(println)

    val wordsFlatMap = book.flatMap(x => x.split("\\W+")) //.foreach(println)

    // Count up the occurrences of each word
    val wordsCount = wordsFlatMap.countByValue()

    // Print the results. Various options commented out below

    // println(wordsCount.take(5))
    //  for (i <- 1 to 5) yield println(wordsCount)
    // wordsCount.take(5).foreach(println)
    //  1.to(15).foreach((wordsCount) => println(wordsCount))


    //improve wordsCount by accounting for capitalization
    val wordsMapLowercase = wordsFlatMap.map(x => x.toLowerCase())

    val wordsCount_caseStandardized = wordsMapLowercase.countByValue()
    //wordsCount_caseStandardized.toSeq.sortBy(_._1).foreach(println)
    val sorted = wordsCount_caseStandardized.toSeq.sortBy(_._1)
   //  wordsCount_caseStandardized.toSeq.sortBy(_._1).foreach(println)


    //sort based on words that start with "s"
    val s_section = sorted.filter(_._1.startsWith("s"))
    val first_item = s_section.take(1).toString()
      //wordsCount_caseStandardized.toSeq.sortBy(_._1).drop(48).take(20).foreach(println)
    //wordsCount_caseStandardized.toSeq.sortBy(_._2).foreach(println)

    s_section.foreach(println)

    println(s"First item in the S Section is $first_item.")




  }
}
