package com.ravi.spark

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object WordCountBetter {
  
  
  
  //Main function where all the action happens
   def main(args: Array[String]) {
    
   // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]","WordCountBetter")
    
    // Read each line of my book into an RDD
    val input = sc.textFile("book.txt")
    
    //Split each line into words
    val words = input.flatMap(x => x.split("\\W+"))
    
    //move all to Lower Case
    val lowerCaseWords = words.map(x => x.toLowerCase())
    
    //Count the number of occurences
    val wordCounts = lowerCaseWords.countByValue()
    
    //Print the results 
    wordCounts.foreach(println)
    
  }
  
}