package com.ravi.spark

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.math.max

object MaxTemperatures {
  
  def parseLine(line: String) ={
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    
    (stationId, entryType, temperature)
    
  }
  
  
  def main(args : Array[String]) {
    
     // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    //Set the sparkContext
    val sc = new SparkContext("local[*]", "MaxTemperatures")
    
    //Read each line
    val lines = sc.textFile("1800.csv")
    
    //Convert each to Tuple (stationId, entryType, value)
    val parsedLines = lines.map(parseLine)
    
    //Filter out only the TMAX value
    val maxTemps = parsedLines.filter(x => x._2 == "TMAX")
    
    
    //Convert to (StationID, Temp) Tuple
    val stationTemps = maxTemps.map( x => (x._1, x._3.toFloat)) 
    
    //Retain only the max Temp using a Reduce Operation 
    
    val maxTempsByStation = stationTemps.reduceByKey((x,y) => max(x,y))
    
    //Perform Action
    
    val results = maxTempsByStation.collect()
    
    //Format and Print results
    
    for (result <- results.sorted) {
       val station = result._1
       val temp = result._2
       val formattedTemp = f"$temp%.2f F"
       println(s"$station max temperature: $formattedTemp") 
    }
    
    
    
  }
}