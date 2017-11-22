package com.ravi.spark.streaming

import Utilities._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark._
import org.netlib.util.Second
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils

/* Listen to tweets and save them to disk */
object SaveTweets {

  def main(args: Array[String]) {

    /*Setup twitter*/
    setupTwitter()

    /*Setup logging*/
    setupLogging()

    /*Setup a new streaming context - Batch Interval 1 sec , use all CPU Cores */
    val ssc = new StreamingContext("local[*]", "SaveTweets", Seconds(1))

    /*Create a DStream of Twitter Objects*/
    val tweets = TwitterUtils.createStream(ssc, None)

    /*Convert the Twitter DStream to a Stream of only Twitter Statuses*/
    val statuses = tweets.map(status => status.getText())

    /* Counter for total number of tweets */
    var totalTweets: Long = 0

    statuses.foreachRDD((rdd, time) => {
      //Only if there is a value in this RDD
      if (rdd.count > 0) {
        //Combine all the partitions to one RDD
        val repartiontionedRDD = rdd.repartition(1).cache()
        //Print as a text file 
        repartiontionedRDD.saveAsTextFile("Tweets_" + time.milliseconds.toString())
        //Stop on 1000 tweets
        totalTweets = repartiontionedRDD.count() + totalTweets
        //Print the total
        println("Total Tweets :" + totalTweets)

        //Exit if totalTweets >1000
        if (totalTweets > 1000) {
          System.exit(0)
        }

       
      }
    })
    
    //Setup a checkpoint directory
    
    ssc.checkpoint("C:/checkpoint")
     // Kick it all off
        ssc.start()
        ssc.awaitTermination()
  }

}