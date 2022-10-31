package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.SparkContext

import scala.math.min


object MinTemperatures_David {
/*
* Again, you create a parseLine function to define how you'll want your map function to chop things up.
* This is your chance to define how you want things divided, and what exactly you'll be taking from the file.
*
* In this example, our 1800.csv looks something like this...
* STATIONID, Date, [TMAX or TMIN for that day],,,,,
* ITE00100554,18000101,TMAX,-75,,,E,
* ITE00100554,18000101,TMIN,-148,,,E,
*
* We'll use parseLine to define precisely what we ant out of these tuples, and it'll serve as the kernel of our map
* function.
* */
  def parseLine(line: String): (String, String, Float) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MinTemperatures_David")


    //read each line of input data
    val data = sc.textFile("data/1800.csv").map(parseLine)

    // Convert to (stationID, entryType, temperature) tuples

    data.foreach(println)
    // Filter out all but TMIN entries
    val minFilter = data.filter(x => x._2 == "TMIN")
    // minFilter.foreach(println)
    // Convert to (stationID, temperature)
   // minFilter.sortBy(x => x._3).foreach(println)
    val stationIDs = minFilter.map(x => (x._1, x._3.toFloat))
    // stationIDs.foreach(println)

    //for each station ID, I want to run a minimum function on its temperature, then output that station ID with the min temp alone)
    val minResults = stationIDs.reduceByKey((id, temp) => min(id, temp))
    // Reduce by stationID retaining the minimum temperature found
    minResults.collect()

    // Collect, format, and print the results
    for (result <- minResults) {
      val stationID = result._1
      val minTemp = result._2
      println(s"$stationID minimum temperature is: $minTemp")

    }
  }


}
