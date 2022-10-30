package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

/** Compute the average number of friends by age in a social network. */
object FriendsByAge {

  /** A function that splits a line of input into (age, numFriends) tuples.
   *  You typically start off with a parseline function that clearly defines how you want your target file chopped up.
   *  The map function basically always calls this function in order to do it's work.
   *  * */
  def parseLine(line: String): (Int, Int) = {
      // Split by commas
      val fields = line.split(",")
      // Extract the age and numFriends fields, and convert to integers; remember to just take what you want
      val age = fields(2).toInt
      val numFriends = fields(3).toInt
      // Create a tuple that is our result.
      (age, numFriends)
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
   val log = Logger.getLogger("org")
     log.setLevel(Level.WARN)
    log.info("Running Cluster")
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByAge")
  
    // Load each line of the source data into an RDD
    val lines = sc.textFile("data/fakefriends-noheader.csv")
    
    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(parseLine)
    
    // Lots going on here...
    // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
    // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
    // adding together all the numFriends values and 1's respectively.
    //val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    val preResults = rdd.mapValues(x => (x, 1))
    log.warn("Pre Reduced RESULTS\r"+ preResults.collect().sorted.foreach(println))
    // x._1+y._1 = how many friends exist for that age | x._2 + y._2 = how many people exist for that age
    // In case what's below is confusing, you could rephrase it as:
    // val totalsByAge = rdd.mapValues(x => (uniqueAge, 1)).reduceByKey( (friends,ageInstances) => (friends._1 + y._1, ageSet._2 + y._2))
    val totalsByAge = preResults.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))



    // So now we have tuples of (age, (totalFriends, totalInstances))
    // To compute the average we divide totalFriends / totalInstances for each age.
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)
    
    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    val results = averagesByAge.collect()


    println("Preparing to Reduce in ten seconds. . .")
    Thread.sleep(10000)

    // Sort and print the final results.
    results.sorted.foreach(println)
  }
    
}
  