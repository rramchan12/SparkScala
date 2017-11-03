package com.ravi.spark

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object WordCountSorted {

  //Main function where all the action happens
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCountSorted")

    // Read each line of my book into an RDD
    val input = sc.textFile("book.txt")

    //Split each line into words
    val words = input.flatMap(x => x.split("\\W+"))

    //move all to Lower Case, 
    val lowerCaseWords = words.map(x =>x.toLowerCase())

    //Create a tuple and reduce
    val wordCounts = lowerCaseWords.map(x => (x,1)).reduceByKey((x,y) => x+y)
    
    
    //Reverse it for sorting
    val wordCountsSorted = wordCounts.map(x => (x._2,x._1)).sortByKey()
    
    for (result <- wordCountsSorted){
      val word = result._2
      val count = result._1
      
      println(s"$word : $count")
    }

   
  }
}