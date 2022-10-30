package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession


object MinTemperaturesSQL_David {

  def parseLine(line: String): (String, String, Float) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }

  case class Temperature(value: String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder.appName("WordCount")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "local")
      .getOrCreate()

    import session.implicits._


    //read each line of input data
    val data = session.read.option("inferSchema", "true").csv("data/1800.csv")
    val temp = data.select("_c1", "_c2", "_c3").where("_c2='TMIN'").orderBy("_c3")
    temp.show()
    // Convert to (stationID, entryType, temperature) tuples
    val tempMin = temp.select("*").first()


    // Filter out all but TMIN entries

    // minFilter.foreach(println)
    // Convert to (stationID, temperature)

    //for each station ID, I want to run a minimum function on its temperature, then output that station ID with the min temp alone)

    // Reduce by stationID retaining the minimum temperature found


    // Collect, format, and print the results
    println(tempMin)
  }


}
