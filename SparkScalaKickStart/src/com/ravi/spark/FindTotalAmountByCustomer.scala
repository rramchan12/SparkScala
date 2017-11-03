package com.ravi.spark

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object FindTotalAmountByCustomer {
  
  def parseLine(line : String) = {
    val values = line.split(",")
    val customerId = values(0).toInt
    val orderValue = values(2).toFloat
    
    (customerId,orderValue)
    
  }
  
  
  def main(arg : Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "FindTotalAmountByCustomer")
    val lines = sc.textFile("customer-orders.csv")
    val parsedLines = lines.map(parseLine)
    val totalAmountByCustomer = parsedLines.reduceByKey(_+_)
    
    val sortedTotalAmountByCustomer = totalAmountByCustomer.map(x => (x._2, x._1)).sortByKey(false)
    
    for (element <- sortedTotalAmountByCustomer){
      val orderTotal = element._1
      val customerId = element._2
      println(s"$customerId : $orderTotal")
    }
    
    
  }
  
}