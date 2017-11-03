package com.ravi.spark.advanced

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object  MostPopularSuperHero {
  
   // Function to extract hero ID -> hero name tuples (or None in case of failure)
  def parseName(line : String) : Option[(Int, String)] = {
    var fields = line.split("\"")
    if (fields.length>1){
      return Some(fields(0).trim().toInt, fields(1))
    }
    else {
      return None
    }
    
  }
  
  /** Find the superhero with the most co-appearances. */
  def countCoOccurences(line: String)  = {
    var elements = line.split("\\s+")
    
    (elements(0).toInt, elements.length - 1)
  }
  
  
  def main(args : Array[String]){
    //Set default level to error
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    //Load the SparkContext
    val sc = new SparkContext("local[*]","MostPopularSuperHero")
    
    //Load the superhero names
    val names = sc.textFile("marvel/Marvel-names.txt")
    val namesRdd = names.flatMap(parseName)
    
    val lines = sc.textFile("marvel/Marvel-graph.txt")
    val pairings = lines.map(countCoOccurences)
    
    val totalFriendsByCharacter = pairings.reduceByKey(_+_)
    
    val flipped = totalFriendsByCharacter.map( x => (x._2,x._1))
    val mostPopular = flipped.max()
    
    //Find the name of the most Popular Movie
    val mostPopularSuperHero = namesRdd.lookup(mostPopular._2)(0)
    
    println(s"mostPopularSuperHero is the most popular Superhero with $mostPopular friends")
    
    

  }
}